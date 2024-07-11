package client

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"time"
)

type PubClient struct {
	networks []*network
}

func NewPubClient(host string, port int, timeout time.Duration, poolSize int) (*PubClient, error) {
	if poolSize < 1 {
		poolSize = 1
	}

	networks := make([]*network, poolSize)
	for i := 0; i < poolSize; i++ {
		nw, err := newNetwork(host, port, timeout)
		if err != nil {
			for j := 0; j < i; j++ {
				networks[j].Close()
			}
			return nil, err
		}
		networks[i] = nw
	}

	return &PubClient{
		networks: networks,
	}, nil
}

func (pc *PubClient) network() (*network, error) {
	nw := pc.networks[rand.Int()%len(pc.networks)]
	return nw, nw.init()
}

func (pc *PubClient) Publish(mqName string, msg *Message, traceId string) error {
	nw, ne := pc.network()
	if ne != nil {
		return ne
	}
	payload := msg.ToBytes()
	payLoadLen := len(payload)
	if payLoadLen <= 8 {
		return errors.New("empty message")
	}
	traceIdLen := len(traceId)
	buf := make([]byte, HeaderSize+len(mqName)+payLoadLen+traceIdLen)
	buf[0] = CommandPub.Byte()
	buf[19] = byte(traceIdLen)
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(mqName)))
	binary.LittleEndian.PutUint32(buf[3:], uint32(payLoadLen))

	dst := buf[HeaderSize:]
	n := copy(dst, mqName)
	dst = dst[n:]
	if traceIdLen > 0 {
		n = copy(dst, traceId)
		dst = dst[n:]
	}
	copy(dst, payload)
	payload = nil

	if err := writeAll(nw.conn, buf, nw.ioTimeout); err != nil {
		nw.Close()
		return err
	}

	buf = nil

	return pc.readResult(0)
}

func (pc *PubClient) PublishDelay(mqName string, msg *Message, delayMils int64, traceId string) error {
	nw, ne := pc.network()
	if ne != nil {
		return ne
	}
	payload := msg.ToBytes()
	payLoadLen := len(payload)
	if payLoadLen <= 8 {
		return errors.New("empty message")
	}
	traceIdLen := len(traceId)
	buf := make([]byte, HeaderSize+len(mqName)+len(payload)+12+traceIdLen)
	buf[0] = CommandDelay.Byte()
	buf[19] = byte(traceIdLen)
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(mqName)))
	binary.LittleEndian.PutUint32(buf[3:], uint32(payLoadLen))

	dst := buf[HeaderSize:]
	n := copy(dst, mqName)
	dst = dst[n:]
	if traceIdLen > 0 {
		n = copy(dst, traceId)
		dst = dst[n:]
	}

	binary.LittleEndian.PutUint64(dst, uint64(delayMils))

	copy(dst[8:], payload)
	payload = nil

	if err := writeAll(nw.conn, buf, nw.ioTimeout); err != nil {
		nw.Close()
		return err
	}

	buf = nil

	return pc.readResult(0)
}

func (pc *PubClient) CreateMQ(mqName string, life int64, traceId string) error {
	nw, ne := pc.network()
	if ne != nil {
		return ne
	}
	traceIdLen := len(traceId)
	hBuf := make([]byte, HeaderSize)
	hBuf[0] = CommandCreateMQ.Byte()
	hBuf[19] = byte(traceIdLen)
	binary.LittleEndian.PutUint16(hBuf[1:], uint16(len(mqName)))
	hBuf = append(hBuf, []byte(mqName)...)

	if traceIdLen > 0 {
		hBuf = append(hBuf, []byte(traceId)...)
	}

	pBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(pBuf, uint64(life))
	hBuf = append(hBuf, pBuf...)

	if err := writeAll(nw.conn, hBuf, nw.ioTimeout); err != nil {
		nw.Close()
		return err
	}

	return pc.readResult(0)
}

func (pc *PubClient) DeleteMQ(mqName string, traceId string) error {
	nw, ne := pc.network()
	if ne != nil {
		return ne
	}
	traceIdLen := len(traceId)
	hBuf := make([]byte, HeaderSize)
	hBuf[0] = CommandDeleteMQ.Byte()
	hBuf[19] = byte(traceIdLen)
	binary.LittleEndian.PutUint16(hBuf[1:], uint16(len(mqName)))
	hBuf = append(hBuf, []byte(mqName)...)
	if traceIdLen > 0 {
		hBuf = append(hBuf, []byte(traceId)...)
	}
	if err := writeAll(nw.conn, hBuf, nw.ioTimeout); err != nil {
		nw.Close()
		return err
	}

	return pc.readResult(0)
}

//func (pc *PubClient) ChangeMqLife(mqName string, life int64, traceId string) error {
//	if err := pc.init(); err != nil {
//		return err
//	}
//	traceIdLen := len(traceId)
//	hBuf := make([]byte, HeaderSize)
//	hBuf[0] = CommandChangeLf.Byte()
//	hBuf[19] = byte(traceIdLen)
//	binary.LittleEndian.PutUint16(hBuf[1:], uint16(len(mqName)))
//	hBuf = append(hBuf, []byte(mqName)...)
//
//	if traceIdLen > 0 {
//		hBuf = append(hBuf, []byte(traceId)...)
//	}
//
//	pBuf := make([]byte, 8)
//	binary.LittleEndian.PutUint64(pBuf, uint64(life))
//	hBuf = append(hBuf, pBuf...)
//
//	if err := writeAll(pc.conn, hBuf, pc.ioTimeout); err != nil {
//		pc.Close()
//		return err
//	}
//
//	return pc.readResult(0)
//}

func (pc *PubClient) GetMqList(traceId string) (string, error) {
	nw, ne := pc.network()
	if ne != nil {
		return "", ne
	}
	traceIdLen := len(traceId)
	hBuf := make([]byte, HeaderSize)
	hBuf[0] = CommandList.Byte()
	hBuf[19] = byte(traceIdLen)
	if traceIdLen > 0 {
		hBuf = append(hBuf, []byte(traceId)...)
	}
	if err := writeAll(nw.conn, hBuf, nw.ioTimeout); err != nil {
		nw.Close()
		return "", err
	}
	return pc.readMqListResult()
}

func (pc *PubClient) Alive(traceId string) error {
	nw, ne := pc.network()
	if ne != nil {
		return ne
	}
	traceIdLen := len(traceId)
	hBuf := make([]byte, HeaderSize)
	hBuf[0] = CommandAlive.Byte()
	hBuf[19] = byte(traceIdLen)
	if traceIdLen > 0 {
		hBuf = append(hBuf, []byte(traceId)...)
	}
	if err := writeAll(nw.conn, hBuf, nw.ioTimeout); err != nil {
		nw.Close()
		return err
	}
	return pc.readResult(0)
}

func (pc *PubClient) Close() {
	for _, nw := range pc.networks {
		nw.Close()
	}
}

func (pc *PubClient) readResult(timeout time.Duration) error {
	nw, ne := pc.network()
	if ne != nil {
		return ne
	}
	if timeout == 0 {
		timeout = nw.ioTimeout
	}
	f := func() error {
		buf := make([]byte, RespHeaderSize)
		if err := readAll(nw.conn, buf, timeout); err != nil {
			return err
		}

		code := binary.LittleEndian.Uint16(buf)
		if code == OkCode || code == AliveCode {
			return nil
		}
		if code != ErrCode {
			return errors.New("not support code")
		}
		errMsgLen := int(binary.LittleEndian.Uint16(buf[2:]))
		return readErrCodeMsg(nw.conn, errMsgLen, nw.ioTimeout)
	}

	var err error
	defer func() {
		if err != nil && !IsBizErr(err) {
			nw.Close()
		}
	}()
	err = f()
	return err
}

func (pc *PubClient) readMqListResult() (string, error) {
	nw, ne := pc.network()
	if ne != nil {
		return "", ne
	}
	f := func() (string, error) {
		buf := make([]byte, RespHeaderSize)
		if err := readAll(nw.conn, buf, nw.ioTimeout); err != nil {
			return "", err
		}

		code := binary.LittleEndian.Uint16(buf)
		if code == OkCode {
			bodyLen := int(binary.LittleEndian.Uint32(buf[2:]))
			return readMQListBody(nw.conn, bodyLen, nw.ioTimeout)
		}
		if code != ErrCode {
			return "", errors.New("not suport code")
		}
		errMsgLen := int(binary.LittleEndian.Uint16(buf[2:]))
		return "", readErrCodeMsg(nw.conn, errMsgLen, nw.ioTimeout)
	}

	var err error
	defer func() {
		if err != nil && !IsBizErr(err) {
			nw.Close()
		}
	}()
	var ret string
	ret, err = f()
	return ret, err
}
