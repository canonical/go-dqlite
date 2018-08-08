package client

import (
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessage_StaticBytesAlignment(t *testing.T) {
	message := Message{}
	message.Init(4096)
	pointer := uintptr(unsafe.Pointer(&message.body1.Bytes))
	assert.Equal(t, pointer%messageWordSize, uintptr(0))
}

func TestMessage_putBlob(t *testing.T) {
	cases := []struct {
		Blob   []byte
		Offset int
	}{
		{[]byte{1, 2, 3, 4, 5}, 8},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, 8},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 16},
	}

	message := Message{}
	message.Init(16)

	for _, c := range cases {
		t.Run(fmt.Sprintf("%d", c.Offset), func(t *testing.T) {
			message.putBlob(c.Blob)

			bytes, offset := message.Body1()

			assert.Equal(t, bytes[:len(c.Blob)], c.Blob)
			assert.Equal(t, offset, c.Offset)

			message.Reset()
		})
	}
}

func TestMessage_putString(t *testing.T) {
	cases := []struct {
		String string
		Offset int
	}{
		{"hello", 8},
		{"hello!!", 8},
		{"hello world", 16},
	}

	message := Message{}
	message.Init(16)

	for _, c := range cases {
		t.Run(c.String, func(t *testing.T) {
			message.putString(c.String)

			bytes, offset := message.Body1()

			assert.Equal(t, string(bytes[:len(c.String)]), c.String)
			assert.Equal(t, offset, c.Offset)

			message.Reset()
		})
	}
}

func TestMessage_putUint8(t *testing.T) {
	message := Message{}
	message.Init(8)

	v := uint8(12)

	message.putUint8(v)

	bytes, offset := message.Body1()

	assert.Equal(t, bytes[0], byte(v))

	assert.Equal(t, offset, 1)
}

func TestMessage_putUint16(t *testing.T) {
	message := Message{}
	message.Init(8)

	v := uint16(666)

	message.putUint16(v)

	bytes, offset := message.Body1()

	assert.Equal(t, bytes[0], byte((v & 0x00ff)))
	assert.Equal(t, bytes[1], byte((v&0xff00)>>8))

	assert.Equal(t, offset, 2)
}

func TestMessage_putUint32(t *testing.T) {
	message := Message{}
	message.Init(8)

	v := uint32(130000)

	message.putUint32(v)

	bytes, offset := message.Body1()

	assert.Equal(t, bytes[0], byte((v & 0x000000ff)))
	assert.Equal(t, bytes[1], byte((v&0x0000ff00)>>8))
	assert.Equal(t, bytes[2], byte((v&0x00ff0000)>>16))
	assert.Equal(t, bytes[3], byte((v&0xff000000)>>24))

	assert.Equal(t, offset, 4)
}

func TestMessage_putUint64(t *testing.T) {
	message := Message{}
	message.Init(8)

	v := uint64(5000000000)

	message.putUint64(v)

	bytes, offset := message.Body1()

	assert.Equal(t, bytes[0], byte((v & 0x00000000000000ff)))
	assert.Equal(t, bytes[1], byte((v&0x000000000000ff00)>>8))
	assert.Equal(t, bytes[2], byte((v&0x0000000000ff0000)>>16))
	assert.Equal(t, bytes[3], byte((v&0x00000000ff000000)>>24))
	assert.Equal(t, bytes[4], byte((v&0x000000ff00000000)>>32))
	assert.Equal(t, bytes[5], byte((v&0x0000ff0000000000)>>40))
	assert.Equal(t, bytes[6], byte((v&0x00ff000000000000)>>48))
	assert.Equal(t, bytes[7], byte((v&0xff00000000000000)>>56))

	assert.Equal(t, offset, 8)
}

func TestMessage_putNamedValues(t *testing.T) {
	message := Message{}
	message.Init(256)

	timestamp, err := time.ParseInLocation("2006-01-02", "2018-08-01", time.UTC)
	require.NoError(t, err)

	values := NamedValues{
		{Ordinal: 1, Value: int64(123)},
		{Ordinal: 2, Value: float64(3.1415)},
		{Ordinal: 3, Value: true},
		{Ordinal: 4, Value: []byte{1, 2, 3, 4, 5, 6}},
		{Ordinal: 5, Value: "hello"},
		{Ordinal: 6, Value: nil},
		{Ordinal: 7, Value: timestamp},
	}

	message.putNamedValues(values)

	bytes, offset := message.Body1()

	assert.Equal(t, 88, offset)
	assert.Equal(t, bytes[0], byte(7))
	assert.Equal(t, bytes[1], byte(bindings.Integer))
	assert.Equal(t, bytes[2], byte(bindings.Float))
	assert.Equal(t, bytes[3], byte(bindings.Boolean))
	assert.Equal(t, bytes[4], byte(bindings.Blob))
	assert.Equal(t, bytes[5], byte(bindings.Text))
	assert.Equal(t, bytes[6], byte(bindings.Null))
	assert.Equal(t, bytes[7], byte(bindings.ISO8601))
}

func TestMessage_putHeader(t *testing.T) {
	message := Message{}
	message.Init(64)

	message.putString("hello")
	message.putHeader(bindings.RequestExec)
}

func BenchmarkMessage_putString(b *testing.B) {
	message := Message{}
	message.Init(4096)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		message.Reset()
		message.putString("hello")
	}
}

func BenchmarkMessage_putUint64(b *testing.B) {
	message := Message{}
	message.Init(4096)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		message.Reset()
		message.putUint64(270)
	}
}

func TestMessage_getString(t *testing.T) {
	cases := []struct {
		String string
		Offset int
	}{
		{"hello", 8},
		{"hello!!", 8},
		{"hello!!!", 16},
		{"hello world", 16},
	}

	for _, c := range cases {
		t.Run(c.String, func(t *testing.T) {
			message := Message{}
			message.Init(16)

			message.putString(c.String)
			message.putHeader(0)

			message.Rewind()

			s := message.getString()

			_, offset := message.Body1()

			assert.Equal(t, s, c.String)
			assert.Equal(t, offset, c.Offset)
		})
	}
}

func TestMessage_getString_Overflow(t *testing.T) {
	message := Message{}
	message.Init(8)

	message.putBlob([]byte("12345678"))
	message.putBlob([]byte{'9', 0, 0, 0, 0, 0, 0, 0})
	message.putHeader(0)

	message.Rewind()

	s := message.getString()
	assert.Equal(t, "123456789", s)

	assert.Equal(t, 8, message.body1.Offset)
	assert.Equal(t, 8, message.body2.Offset)
}

// The overflowing string ends exactly at word boundary.
func TestMessage_getString_Overflow_WordBoundary(t *testing.T) {
	message := Message{}
	message.Init(8)

	message.putBlob([]byte("abcdefgh"))
	message.putBlob([]byte("ilmnopqr"))
	message.putBlob([]byte{0, 0, 0, 0, 0, 0, 0})
	message.putHeader(0)

	message.Rewind()

	s := message.getString()
	assert.Equal(t, "abcdefghilmnopqr", s)

	assert.Equal(t, 8, message.body1.Offset)
	assert.Equal(t, 16, message.body2.Offset)
}
