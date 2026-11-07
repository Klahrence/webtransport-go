package webtransport

import (
	"encoding/binary"
	"encoding/csv"
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

	covertConfig       *CovertConfig
	receivedSecretData []byte
	previousMaxData    int64
	fileReceived       bool
}

func newOutgoingDataFlowController(covertConfig *CovertConfig) *outgoingDataFlowController {
	return &outgoingDataFlowController{
		maxData:       covertConfig.InitialMaxData,
		lastBlockedAt: -1,
		updated:       make(chan struct{}),

		covertConfig:       covertConfig,
		receivedSecretData: []byte(""),
		previousMaxData:    covertConfig.InitialMaxData,
		fileReceived:       false,
	}
}

func (f *outgoingDataFlowController) AddBytesSent(n int64) int64 {
	f.mx.Lock()
	defer f.mx.Unlock()

	// detect maxData raises and trigger decode when there is a new value
	if f.covertConfig.DoCovert && f.maxData > f.previousMaxData {
		increment := f.maxData - f.previousMaxData - f.covertConfig.Offset
		msg := fmt.Sprintf("maxData was raised from %d to %d\n increment is %d", f.previousMaxData, f.maxData, increment)
		f.previousMaxData = f.maxData
		if increment > -1 {
			fmt.Println(msg)
			f.receiveSecretMessage(increment)
		}
		if increment < 0 && f.fileReceived == false {
			fmt.Printf("RECEIVED SECRET FILE\n")
			os.WriteFile("../receivedSecretFile.txt", f.receivedSecretData, 0o644)
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
	chunk := make([]byte, f.covertConfig.CovertByteCount)
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

	covertConfig       *CovertConfig
	secretData         []byte
	lastIndex          int
	timeStart          time.Time
	receivedCovertData int64
	timeMeasured       bool
	file               *os.File
	writer             *csv.Writer
}

func readFile() []byte {
	file, err := os.ReadFile("../secretFile.txt")
	if err != nil {
		log.Fatalf("Could not read file: %v\n", err)
	}
	fmt.Printf("Bytes read: %d\n", len(file))
	return file
}

func newIncomingDataFlowController(bytesRead int64, covertConfig *CovertConfig, queueWindowUpdate func(int64)) *incomingDataFlowController {
	secret := readFile()

	var file *os.File
	var writer *csv.Writer
	var err error

	fmt.Printf("covertConfig %v\n", covertConfig)

	if covertConfig.RecordToFile {
		file, err = os.Create(covertConfig.OutputFilePath)
		if err != nil {
			log.Fatal(err)
		}

		writer = csv.NewWriter(file)
		err = writer.Write([]string{
			"Use Covert Channel",
			"Client bitrate",
			"Count of bytes per increment",
			"Initial MaxData",
			"MaxData",
			"Delta",
			"Time (ms)",
		})
		writer.Flush()

	}
	return &incomingDataFlowController{
		bytesRead:         bytesRead,
		maxData:           covertConfig.InitialMaxData,
		receiveWindow:     covertConfig.InitialMaxData - bytesRead,
		queueWindowUpdate: queueWindowUpdate,

		covertConfig:       covertConfig,
		secretData:         secret,
		lastIndex:          0,
		receivedCovertData: 0,
		timeMeasured:       false,
		file:               file,
		writer:             writer,
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
	var maxData int64

	if f.covertConfig.DoCovert {
		increment := f.GetIncrement()
		maxData = f.maxData + f.covertConfig.Offset + increment // add increment and use maxData instead of bytesRead

		if !f.timeMeasured {
			if increment > -1 {
				fmt.Printf(" maxData: %d -> %d\n Read bytes: %d\n ReceiveWindow: %d\n Increment: %d\n", f.maxData, maxData, f.bytesRead, f.receiveWindow, increment)
			} else {
				elapsedTime := time.Since(f.timeStart)
				receivedCovertDataBPS := (float64(f.lastIndex) * 8) / elapsedTime.Seconds()
				receivedVideoDataBPS := (float64(f.bytesRead) * 8) / elapsedTime.Seconds()
				fmt.Printf("Received COVERT DATA in bytes per second: %f\n", receivedCovertDataBPS)
				fmt.Printf("Received VIDEODATA in bytes per second: %f\n", receivedVideoDataBPS)
				fmt.Printf("Received VIDEODATA in bytes: %d\n", f.bytesRead)
				fmt.Printf("Elapsed time: \n", elapsedTime.Seconds())
				f.timeMeasured = true
				f.file.Close()
				f.file = nil
				f.writer = nil
			}
		}
	} else {
		maxData = f.bytesRead + f.receiveWindow
	}

	if f.covertConfig.RecordToFile && !f.timeMeasured {
		fmt.Printf("Print to file....\n")
		f.writer.Write([]string{
			fmt.Sprint(f.covertConfig.DoCovert),
			fmt.Sprint(f.covertConfig.ClientBitrate),
			fmt.Sprint(f.covertConfig.CovertByteCount),
			fmt.Sprint(f.covertConfig.InitialMaxData),
			fmt.Sprint(maxData),
			fmt.Sprint(maxData - f.maxData),
			fmt.Sprint(float64(time.Since(f.timeStart)) / float64(time.Millisecond)),
		})
		f.writer.Flush()
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
	chunk := make([]byte, f.covertConfig.CovertByteCount)
	for idx := 0; idx < f.covertConfig.CovertByteCount; idx++ {
		newIndex := f.lastIndex + idx
		if newIndex >= len(f.secretData) {
			chunk[idx] = '\x00'
		} else {
			chunk[idx] = f.secretData[f.lastIndex+idx]
		}
	}
	increment := int64(binary.BigEndian.Uint16(chunk))
	f.lastIndex += f.covertConfig.CovertByteCount

	return increment
}
