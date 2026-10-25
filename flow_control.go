package webtransport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

var errMaxDataNotIncreased = errors.New("webtransport: WT_MAX_DATA capsule didn't increase data limit")

type outgoingDataFlowController struct {
	mx            sync.Mutex
	maxData       int64
	bytesSent     int64
	lastBlockedAt int64
	updated       chan struct{}

	receivedSecretData []byte
	chunkSize          int
	initialMaxData     int64
	previousMaxData    int64
	fileReceived       bool
}

func newOutgoingDataFlowController(maxData int64) *outgoingDataFlowController {
	return &outgoingDataFlowController{
		maxData:       maxData,
		lastBlockedAt: -1,
		updated:       make(chan struct{}),

		receivedSecretData: []byte(""),
		chunkSize:          2,
		previousMaxData:    maxData,
		initialMaxData:     maxData,
		fileReceived:       false,
	}
}

func (f *outgoingDataFlowController) AddBytesSent(n int64) int64 {
	f.mx.Lock()
	defer f.mx.Unlock()

	// detect maxData raises and trigger decode when there is a new value
	if f.maxData > f.previousMaxData {
		increment := f.maxData - f.previousMaxData - f.initialMaxData
		msg := fmt.Sprintf("maxData was raised from %d to %d\n increment is %d", f.previousMaxData, f.maxData, increment)
		f.previousMaxData = f.maxData
		if increment > -1 {
			fmt.Println(msg)
			f.receiveSecretMessage(increment)
		}
		if increment < 0 && f.fileReceived == false {
			fmt.Printf("RECEIVED SECRET FILE\n")
			os.WriteFile("../receivedSecretFile.pdf", f.receivedSecretData, 0o644)
			f.fileReceived = true
		}
	}

	var added int64
	if f.bytesSent < f.maxData {
		added = min(n, f.maxData-f.bytesSent)
	}
	f.bytesSent += added
	return added
}

func (f *outgoingDataFlowController) receiveSecretMessage(increment int64) {
	chunk := make([]byte, f.chunkSize)
	binary.BigEndian.PutUint16(chunk, uint16(increment))
	f.receivedSecretData = append(f.receivedSecretData, chunk...)
	fmt.Print("Received data chunk!")
	// fmt.Printf(" Received Data: %s\n", string(f.receivedSecretData))
}

func (f *outgoingDataFlowController) IsNewlyBlocked() (bool, int64) {
	f.mx.Lock()
	defer f.mx.Unlock()

	if f.bytesSent < f.maxData || f.maxData == f.lastBlockedAt {
		return false, 0
	}
	f.lastBlockedAt = f.maxData
	return true, f.maxData
}

func (f *outgoingDataFlowController) UpdateMaxData(maxData int64) error {
	f.mx.Lock()
	defer f.mx.Unlock()

	if maxData <= f.maxData {
		return fmt.Errorf("%w: current limit: %d, received limit: %d", errMaxDataNotIncreased, f.maxData, maxData)
	}
	f.maxData = maxData
	close(f.updated)
	f.updated = make(chan struct{})
	return nil
}

func (f *outgoingDataFlowController) NextUpdate() <-chan struct{} {
	f.mx.Lock()
	updated := f.updated
	f.mx.Unlock()
	return updated
}

type incomingDataFlowController struct {
	mx sync.Mutex

	bytesRead         int64
	maxData           int64
	receiveWindow     int64
	queueWindowUpdate func(int64)

	secretData         []byte
	chunkSize          int
	lastIndex          int
	timeStart          time.Time
	receivedCovertData int64
	timeMeasured       bool
}

func readFile() []byte {
	file, err := os.ReadFile("../steganography.pdf")
	if err != nil {
		log.Fatalf("Could not read file: %v\n", err)
	}
	fmt.Printf("Bytes read: %d\n", len(file))
	return file
}

func newIncomingDataFlowController(bytesRead, maxData int64, queueWindowUpdate func(int64)) *incomingDataFlowController {
	// secret := []byte("Hier ist das Geheimnis! :)")
	secret := readFile()
	fmt.Println(string(secret))
	return &incomingDataFlowController{
		bytesRead:         bytesRead,
		maxData:           maxData,
		receiveWindow:     maxData - bytesRead,
		queueWindowUpdate: queueWindowUpdate,

		secretData:         secret,
		chunkSize:          2,
		lastIndex:          0,
		receivedCovertData: 0,
		timeMeasured:       false,
	}
}

func (f *incomingDataFlowController) AddBytesRead(n int64) error {
	f.mx.Lock()
	defer f.mx.Unlock()
	if f.timeStart.IsZero() {
		f.timeStart = time.Now()
	}

	if n > f.maxData-f.bytesRead {
		return fmt.Errorf("webtransport: received more than %d bytes of stream data", f.maxData)
	}
	f.bytesRead += n
	if f.maxData-f.bytesRead > f.receiveWindow-f.receiveWindow/4 {
		return nil
	}
	increment := f.GetIncrement()
	maxData := f.maxData + f.receiveWindow + increment // add increment instead of receiveWindow

	if !f.timeMeasured {
		if increment > -1 {
			fmt.Printf(" maxData: %d -> %d\n Read bytes: %d\n ReceiveWindow: %d\n Increment: %d\n", f.maxData, maxData, f.bytesRead, f.receiveWindow, increment)
		} else {
			elapsedTime := time.Since(f.timeStart)
			receivedCovertDataInBytesPerSecond := (float64(f.lastIndex) * 8) / elapsedTime.Seconds()
			fmt.Printf("Bytes per second: %f\n", receivedCovertDataInBytesPerSecond)
			fmt.Printf("Elapsed time: \n", elapsedTime.Seconds())
			f.timeMeasured = true
		}
	}
	if maxData == f.maxData {
		return nil
	}
	f.maxData = maxData
	f.queueWindowUpdate(maxData)
	return nil
}

func (f *incomingDataFlowController) GetIncrement() int64 {
	// when entire secret is transmitted, the return value is -1. MaxData then is always increased by 9999.
	if f.lastIndex >= len(f.secretData) {
		return -1
	}
	chunk := make([]byte, f.chunkSize)
	for idx := 0; idx < f.chunkSize; idx++ {
		newIndex := f.lastIndex + idx
		if newIndex >= len(f.secretData) {
			chunk[idx] = '\x00'
		} else {
			chunk[idx] = f.secretData[f.lastIndex+idx]
		}
	}
	increment := int64(binary.BigEndian.Uint16(chunk))
	f.lastIndex += f.chunkSize

	return increment
}
