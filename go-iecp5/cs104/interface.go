// Copyright 2020 thinkgos (thinkgo@aliyun.com).  All rights reserved.
// Use of this source code is governed by a version 3 of the GNU General
// Public License, license that can be found in the LICENSE file.

package cs104

import (
	"time"

	"github.com/walkmiao/flypig-iec104-protocol/go-iecp5/asdu"
)

type RawFrameDirection uint8

const (
	RawFrameDirectionUnknown RawFrameDirection = 0
	RawFrameDirectionTX      RawFrameDirection = 1
	RawFrameDirectionRX      RawFrameDirection = 2
)

type RawFrame struct {
	Direction RawFrameDirection
	FrameKind string
	APDU      []byte
	At        time.Time
}

type RawFrameObserver func(frame RawFrame)

// ServerHandlerInterface is the interface of server handler
type ServerHandlerInterface interface {
	InterrogationHandler(asdu.Connect, *asdu.ASDU, asdu.QualifierOfInterrogation) error
	CounterInterrogationHandler(asdu.Connect, *asdu.ASDU, asdu.QualifierCountCall) error
	ReadHandler(asdu.Connect, *asdu.ASDU, asdu.InfoObjAddr) error
	ClockSyncHandler(asdu.Connect, *asdu.ASDU, time.Time) error
	ResetProcessHandler(asdu.Connect, *asdu.ASDU, asdu.QualifierOfResetProcessCmd) error
	DelayAcquisitionHandler(asdu.Connect, *asdu.ASDU, uint16) error
	ASDUHandler(asdu.Connect, *asdu.ASDU) error
}

// ClientHandlerInterface  is the interface of client handler
type ClientHandlerInterface interface {
	InterrogationHandler(asdu.Connect, *asdu.ASDU) error
	CounterInterrogationHandler(asdu.Connect, *asdu.ASDU) error
	ReadHandler(asdu.Connect, *asdu.ASDU) error
	TestCommandHandler(asdu.Connect, *asdu.ASDU) error
	ClockSyncHandler(asdu.Connect, *asdu.ASDU) error
	ResetProcessHandler(asdu.Connect, *asdu.ASDU) error
	DelayAcquisitionHandler(asdu.Connect, *asdu.ASDU) error
	ASDUHandler(asdu.Connect, *asdu.ASDU) error
}
