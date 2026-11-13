package cs104

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestPeerTCPCloseResetsStateBeforeConnectionLostCallback(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	option := NewOption()
	client := NewClient(nil, option)
	client.conn = clientConn

	callbackConnected := make(chan bool, 2)
	client.SetConnectionLostHandler(func(c *Client) {
		callbackConnected <- c.IsConnected()
	})

	done := make(chan struct{})
	go func() {
		client.run(context.Background())
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for !client.IsConnected() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !client.IsConnected() {
		t.Fatal("client did not enter connected state")
	}

	if err := serverConn.Close(); err != nil {
		t.Fatalf("close peer TCP connection: %v", err)
	}

	select {
	case connected := <-callbackConnected:
		if connected {
			t.Fatal("connection-lost callback observed stale connected=true")
		}
	case <-time.After(time.Second):
		t.Fatal("connection-lost callback was not propagated after peer TCP close")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("client run loop did not stop after peer TCP close")
	}

	select {
	case <-callbackConnected:
		t.Fatal("connection-lost callback was invoked more than once")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestReceiveQueuesAbsorbTransientBackpressureAndStillObservePeerClose(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	option := NewOption()
	client := NewClient(nil, option)
	client.conn = clientConn
	client.ctx, client.cancel = context.WithCancel(context.Background())
	client.setConnectStatus(connected)

	oldRawQueueCapacity := int(option.config.RecvUnAckLimitW) << 5
	oldASDUQueueCapacity := int(option.config.RecvUnAckLimitW) << 4
	if cap(client.rcvRaw) <= oldRawQueueCapacity {
		t.Fatalf("raw receive queue was not enlarged: old=%d new=%d", oldRawQueueCapacity, cap(client.rcvRaw))
	}
	if cap(client.rcvASDU) <= oldASDUQueueCapacity {
		t.Fatalf("ASDU receive queue was not enlarged: old=%d new=%d", oldASDUQueueCapacity, cap(client.rcvASDU))
	}

	// Reproduce a backlog that would have filled the original queue. The larger
	// decoupling buffer must absorb it without waiting for an arbitrary timeout.
	for i := 0; i < oldRawQueueCapacity; i++ {
		client.rcvRaw <- []byte{startFrame, 4, uTestFrConfirm, 0, 0, 0}
	}

	disconnectedCh := make(chan struct{})
	var disconnectOnce sync.Once
	notifyDisconnected := func() {
		disconnectOnce.Do(func() {
			client.setConnectStatus(disconnected)
			client.cancel()
			close(disconnectedCh)
		})
	}

	client.wg.Add(1)
	go client.recvLoop(clientConn, notifyDisconnected)

	if _, err := serverConn.Write(newUFrame(uTestFrConfirm)); err != nil {
		t.Fatalf("write peer frame: %v", err)
	}
	if err := serverConn.Close(); err != nil {
		t.Fatalf("close peer TCP connection: %v", err)
	}

	select {
	case <-disconnectedCh:
		if client.IsConnected() {
			t.Fatal("client remained connected after peer TCP close")
		}
	case <-time.After(time.Second):
		t.Fatal("transient receive backlog prevented TCP close propagation")
	}
}

func TestReceiveQueueExhaustionRecyclesSessionWithoutBlockingSocketReader(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	option := NewOption()
	client := NewClient(nil, option)
	client.conn = clientConn
	client.ctx, client.cancel = context.WithCancel(context.Background())
	client.setConnectStatus(connected)

	for len(client.rcvRaw) < cap(client.rcvRaw) {
		client.rcvRaw <- []byte{startFrame, 4, uTestFrConfirm, 0, 0, 0}
	}

	disconnectedCh := make(chan struct{})
	var disconnectOnce sync.Once
	notifyDisconnected := func() {
		disconnectOnce.Do(func() {
			client.setConnectStatus(disconnected)
			client.cancel()
			close(disconnectedCh)
		})
	}

	client.wg.Add(1)
	go client.recvLoop(clientConn, notifyDisconnected)

	if _, err := serverConn.Write(newUFrame(uTestFrConfirm)); err != nil {
		t.Fatalf("write peer frame: %v", err)
	}

	select {
	case <-disconnectedCh:
		if client.IsConnected() {
			t.Fatal("client remained connected after receive queue exhaustion")
		}
	case <-time.After(time.Second):
		t.Fatal("exhausted receive queue blocked the socket reader")
	}
}

func TestTXRawObserverRunsOnlyAfterCompleteSocketWrite(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	observed := make(chan RawFrame, 1)
	option := NewOption().SetRawFrameObserver(func(frame RawFrame) {
		observed <- frame
	})
	client := NewClient(nil, option)
	client.ctx, client.cancel = context.WithCancel(context.Background())

	disconnected := make(chan struct{})
	var disconnectOnce sync.Once
	client.wg.Add(1)
	go client.sendLoop(clientConn, func() {
		disconnectOnce.Do(func() { close(disconnected) })
	})

	apdu := newUFrame(uTestFrActive)
	received := make(chan []byte, 1)
	go func() {
		buffer := make([]byte, len(apdu))
		if _, err := io.ReadFull(serverConn, buffer); err == nil {
			received <- buffer
		}
	}()
	client.sendRaw <- apdu

	select {
	case frame := <-observed:
		if frame.Direction != RawFrameDirectionTX {
			t.Fatalf("unexpected raw direction: %v", frame.Direction)
		}
		if !bytes.Equal(frame.APDU, apdu) {
			t.Fatalf("unexpected observed APDU: % X", frame.APDU)
		}
	case <-time.After(time.Second):
		t.Fatal("successful socket write was not observed")
	}

	select {
	case raw := <-received:
		if !bytes.Equal(raw, apdu) {
			t.Fatalf("unexpected peer bytes: % X", raw)
		}
	case <-time.After(time.Second):
		t.Fatal("peer did not receive APDU")
	}

	client.cancel()
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("send loop did not stop")
	}
}

func TestTXRawObserverDoesNotReportFailedSocketWrite(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	if err := serverConn.Close(); err != nil {
		t.Fatalf("close peer: %v", err)
	}
	defer clientConn.Close()

	observed := make(chan RawFrame, 1)
	option := NewOption().SetRawFrameObserver(func(frame RawFrame) {
		observed <- frame
	})
	client := NewClient(nil, option)
	client.ctx, client.cancel = context.WithCancel(context.Background())
	defer client.cancel()

	disconnected := make(chan struct{})
	var disconnectOnce sync.Once
	client.wg.Add(1)
	go client.sendLoop(clientConn, func() {
		disconnectOnce.Do(func() { close(disconnected) })
	})

	client.sendRaw <- newUFrame(uTestFrActive)
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("failed socket write did not recycle the connection")
	}

	select {
	case frame := <-observed:
		t.Fatalf("failed socket write was incorrectly reported as TX: % X", frame.APDU)
	case <-time.After(30 * time.Millisecond):
	}
}
