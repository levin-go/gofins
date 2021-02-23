package fins

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const DEFAULT_RESPONSE_TIMEOUT = 20 // ms

// Client Omron FINS client
type Client struct {
	conn *net.UDPConn
	resp []chan response
	sync.Mutex
	dst               finsAddress
	src               finsAddress
	sid               byte
	closed            bool
	responseTimeoutMs time.Duration
	byteOrder         binary.ByteOrder
}

// NewClient creates a new Omron FINS client
func NewClient(localAddr, plcAddr Address) (*Client, error) {
	c := new(Client)
	c.dst = plcAddr.finsAddress
	c.src = localAddr.finsAddress
	c.responseTimeoutMs = DEFAULT_RESPONSE_TIMEOUT
	c.byteOrder = binary.BigEndian

	conn, err := net.DialUDP("udp", localAddr.udpAddress, plcAddr.udpAddress)
	if err != nil {
		return nil, err
	}
	c.conn = conn

	c.resp = make([]chan response, 256) //storage for all responses, sid is byte - only 256 values
	go c.listenLoop()
	return c, nil
}
// Set byte order
// Default value: binary.BigEndian
func (c *Client) SetByteOrder(o binary.ByteOrder) {
	c.byteOrder = o
}

// Set response timeout duration (ms).
// Default value: 20ms.
// A timeout of zero can be used to block indefinitely.
func (c *Client) SetTimeoutMs(t uint) {
	c.responseTimeoutMs = time.Duration(t)
}

// Close Closes an Omron FINS connection
func (c *Client) Close() {
	c.closed = true
	c.conn.Close()
}

// ReadWords Reads words from the PLC data area
func (c *Client) ReadWords(memoryArea byte, address uint16, readCount uint16) ([]byte, int) {
	if checkIsWordMemoryArea(memoryArea) == false {
		//return nil, IncompatibleMemoryAreaError{memoryArea}
		fmt.Println(IncompatibleMemoryAreaError{memoryArea})
		return nil, 9991
	}
	command := readCommand(memAddr(memoryArea, address), readCount)
	//fmt.Printf("send command:")
	//for _, v := range command{
	//	fmt.Printf("%02x ", v)
	//}
	//fmt.Println()
	r, e := c.sendCommand(command)
	code := checkResponse(r, e)
	if code != 0 {
		return nil, code
	}

	//data := make([]uint16, readCount, readCount)
	//for i := 0; i < int(readCount); i++ {
	//	data[i] = c.byteOrder.Uint16(r.data[i*2 : i*2+2])
	//}

	return r.data, 0
}

// ReadBytes Reads bytes from the PLC data area
func (c *Client) ReadBytes(memoryArea byte, address uint16, readCount uint16) ([]byte, int) {
	if checkIsWordMemoryArea(memoryArea) == false {
		//return nil, IncompatibleMemoryAreaError{memoryArea}
		fmt.Println(IncompatibleMemoryAreaError{memoryArea})
		return nil,  9991
	}
	command := readCommand(memAddr(memoryArea, address), readCount)
	r, e := c.sendCommand(command)
	code := checkResponse(r, e)
	if code != 0 {
		return nil, code
	}

	return r.data, 0
}

// ReadString Reads a string from the PLC data area
func (c *Client) ReadString(memoryArea byte, address uint16, readCount uint16) (string, int) {
	data, code := c.ReadBytes(memoryArea, address, readCount)
	if code != 0 {
		return "", code
	}
	n := bytes.IndexByte(data, 0)
	if n == -1 {
		n = len(data)
	}
	return string(data[:n]), 0
}

// ReadBits Reads bits from the PLC data area
func (c *Client) ReadBits(memoryArea byte, address uint16, bitOffset byte, readCount uint16) ([]byte, int) {
	if checkIsBitMemoryArea(memoryArea) == false {
		//return nil, IncompatibleMemoryAreaError{memoryArea}
		fmt.Println(IncompatibleMemoryAreaError{memoryArea})
		return nil,  9991
	}
	command := readCommand(memAddrWithBitOffset(memoryArea, address, bitOffset), readCount)
	r, e := c.sendCommand(command)
	code := checkResponse(r, e)
	if code != 0 {
		return nil, code
	}

	data := make([]byte, readCount, readCount)
	for i := 0; i < int(readCount); i++ {
		data[i] = r.data[i]&0x01
	}

	return data, 0
}

// ReadClock Reads the PLC clock
func (c *Client) ReadClock() (*time.Time, int) {
	r, e := c.sendCommand(clockReadCommand())
	code := checkResponse(r, e)
	if code != 0 {
		return nil, code
	}
	year, _ := decodeBCD(r.data[0:1])
	if year < 50 {
		year += 2000
	} else {
		year += 1900
	}
	month, _ := decodeBCD(r.data[1:2])
	day, _ := decodeBCD(r.data[2:3])
	hour, _ := decodeBCD(r.data[3:4])
	minute, _ := decodeBCD(r.data[4:5])
	second, _ := decodeBCD(r.data[5:6])

	t := time.Date(
		int(year), time.Month(month), int(day), int(hour), int(minute), int(second),
		0, // nanosecond
		time.Local,
	)
	return &t, 0
}

// WriteWords Writes words to the PLC data area
func (c *Client) WriteWords(memoryArea byte, address uint16, data []uint16) int {
	if checkIsWordMemoryArea(memoryArea) == false {
		//return IncompatibleMemoryAreaError{memoryArea}
		fmt.Println(IncompatibleMemoryAreaError{memoryArea})
		return 9991
	}
	l := uint16(len(data))
	bts := make([]byte, 2*l, 2*l)
	for i := 0; i < int(l); i++ {
		c.byteOrder.PutUint16(bts[i*2:i*2+2], data[i])
	}
	command := writeCommand(memAddr(memoryArea, address), l, bts)

	return checkResponse(c.sendCommand(command))
}

// WriteString Writes a string to the PLC data area
func (c *Client) WriteString(memoryArea byte, address uint16, s string) int {
	if checkIsWordMemoryArea(memoryArea) == false {
		//return IncompatibleMemoryAreaError{memoryArea}
		fmt.Println(IncompatibleMemoryAreaError{memoryArea})
		return 9991
	}
	bts := make([]byte, 2*len(s), 2*len(s))
	copy(bts, s)

	command := writeCommand(memAddr(memoryArea, address), uint16((len(s)+1)/2), bts) //TODO: test on real PLC

	return checkResponse(c.sendCommand(command))
}

// WriteBytes Writes bytes array to the PLC data area
func (c *Client) WriteBytes(memoryArea byte, address uint16, b []byte) int {
	if checkIsWordMemoryArea(memoryArea) == false {
		//return IncompatibleMemoryAreaError{memoryArea}
		fmt.Println(IncompatibleMemoryAreaError{memoryArea})
		return 9991
	}
	command := writeCommand(memAddr(memoryArea, address), uint16(len(b)/2), b)
	return checkResponse(c.sendCommand(command))
}

// WriteBits Writes bits to the PLC data area
func (c *Client) WriteBits(memoryArea byte, address uint16, bitOffset byte, data []byte) int {
	if checkIsBitMemoryArea(memoryArea) == false {
		return int(memoryArea)
	}
	l := uint16(len(data))
	//bts := make([]byte, 0, l)
	//var d byte
	//for i := 0; i < int(l); i++ {
	//	if data[i] {
	//		d = 0x01
	//	} else {
	//		d = 0x00
	//	}
	//	bts = append(bts, d)
	//}
	command := writeCommand(memAddrWithBitOffset(memoryArea, address, bitOffset), l, data)
	return checkResponse(c.sendCommand(command))
}

// SetBit Sets a bit in the PLC data area
func (c *Client) SetBit(memoryArea byte, address uint16, bitOffset byte) int {
	return c.bitTwiddle(memoryArea, address, bitOffset, 0x01)
}

// ResetBit Resets a bit in the PLC data area
func (c *Client) ResetBit(memoryArea byte, address uint16, bitOffset byte) int {
	return c.bitTwiddle(memoryArea, address, bitOffset, 0x00)
}

// ToggleBit Toggles a bit in the PLC data area
func (c *Client) ToggleBit(memoryArea byte, address uint16, bitOffset byte) int {
	b, e := c.ReadBits(memoryArea, address, bitOffset, 1)
	if e != 0 {
		return e
	}
	//var t byte
	//if b[0] == 0x00{
	//	t = 0x00
	//} else {
	//	t = 0x01
	//}
	return c.bitTwiddle(memoryArea, address, bitOffset, b[0])
}

func (c *Client) bitTwiddle(memoryArea byte, address uint16, bitOffset byte, value byte) int {
	if checkIsBitMemoryArea(memoryArea) == false {
		//return IncompatibleMemoryAreaError{memoryArea}
		fmt.Println(IncompatibleMemoryAreaError{memoryArea})
		return 9991
	}
	mem := memoryAddress{memoryArea, address, bitOffset}
	command := writeCommand(mem, 1, []byte{value})

	return checkResponse(c.sendCommand(command))
}

func checkResponse(r *response, e error) int {
	if e != nil {
		fmt.Println(e)
		return 999
	}
	if r.endCode != EndCodeNormalCompletion {
		//return fmt.Errorf("error reported by destination, end code 0x%x", r.endCode)
		return int(r.endCode)
	}
	return 0
}

func (c *Client) nextHeader() *Header {
	sid := c.incrementSid()
	header := defaultCommandHeader(c.src, c.dst, sid)
	return &header
}

func (c *Client) incrementSid() byte {
	c.Lock() //thread-safe sid incrementation
	c.sid++
	sid := c.sid
	c.Unlock()
	c.resp[sid] = make(chan response) //clearing cell of storage for new response
	return sid
}

func (c *Client) sendCommand(command []byte) (*response, error) {
	header := c.nextHeader()
	bts := encodeHeader(*header)
	bts = append(bts, command...)
	_, err := (*c.conn).Write(bts)
	if err != nil {
		return nil, err
	}

	// if response timeout is zero, block indefinitely
	if c.responseTimeoutMs > 0 {
		select {
		case resp := <-c.resp[header.serviceID]:
			return &resp, nil
		case <-time.After(c.responseTimeoutMs * time.Millisecond):
			return nil, ResponseTimeoutError{c.responseTimeoutMs}
		}
	} else {
		resp := <-c.resp[header.serviceID]
		return &resp, nil
	}
}

func (c *Client) listenLoop() {
	for {
		buf := make([]byte, 2048)
		n, err := bufio.NewReader(c.conn).Read(buf)
		if err != nil {
			// do not complain when connection is closed by user
			if !c.closed {
				log.Fatal(err)
			}
			break
		}

		if n > 0 {
			//fmt.Println("response:")
			//for _,v := range buf[:n]{
			//	fmt.Printf("%02x ", v)
			//}
			//fmt.Println()
			ans := decodeResponse(buf[:n])
			c.resp[ans.header.serviceID] <- ans
		} else {
			log.Println("cannot read response: ", buf)
		}
	}
}

func checkIsWordMemoryArea(memoryArea byte) bool {
	if memoryArea == MemoryAreaDMWord ||
		memoryArea == MemoryAreaARWord ||
		memoryArea == MemoryAreaHRWord ||
		memoryArea == MemoryAreaWRWord ||
		memoryArea == MemoryAreaCIOWord{
		return true
	}
	return false
}

func checkIsBitMemoryArea(memoryArea byte) bool {
	if memoryArea == MemoryAreaDMBit ||
		memoryArea == MemoryAreaARBit ||
		memoryArea == MemoryAreaHRBit ||
		memoryArea == MemoryAreaWRBit ||
		memoryArea == MemoryAreaTaskBit{
		return true
	}
	return false
}
