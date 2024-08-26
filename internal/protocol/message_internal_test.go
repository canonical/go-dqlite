package protocol

import (
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessage_StaticBytesAlignment(t *testing.T) {
	message := Message{}
	message.Init(4096)
	pointer := uintptr(unsafe.Pointer(&message.body.Bytes[0]))
	assert.Equal(t, uintptr(0), pointer%messageWordSize)
}

func TestMessage_putBlob(t *testing.T) {
	cases := []struct {
		Blob   []byte
		Offset int
	}{
		{[]byte{1, 2, 3, 4, 5}, 16},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, 16},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 24},
	}

	message := Message{}
	message.Init(64)

	for _, c := range cases {
		t.Run(fmt.Sprintf("%d", c.Offset), func(t *testing.T) {
			message.PutBlob(c.Blob)

			bytes, offset := message.Body()

			assert.Equal(t, bytes[8:len(c.Blob)+8], c.Blob)
			assert.Equal(t, offset, c.Offset)

			message.reset()
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
			message.PutString(c.String)

			bytes, offset := message.Body()

			assert.Equal(t, string(bytes[:len(c.String)]), c.String)
			assert.Equal(t, offset, c.Offset)

			message.reset()
		})
	}
}

func TestMessage_putUint8(t *testing.T) {
	message := Message{}
	message.Init(8)

	v := uint8(12)

	message.PutUint8(v)

	bytes, offset := message.Body()

	assert.Equal(t, bytes[0], byte(v))

	assert.Equal(t, offset, 1)
}

func TestMessage_putUint16(t *testing.T) {
	message := Message{}
	message.Init(8)

	v := uint16(666)

	message.PutUint16(v)

	bytes, offset := message.Body()

	assert.Equal(t, bytes[0], byte((v & 0x00ff)))
	assert.Equal(t, bytes[1], byte((v&0xff00)>>8))

	assert.Equal(t, offset, 2)
}

func TestMessage_putUint32(t *testing.T) {
	message := Message{}
	message.Init(8)

	v := uint32(130000)

	message.PutUint32(v)

	bytes, offset := message.Body()

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

	message.PutUint64(v)

	bytes, offset := message.Body()

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

	bytes, offset := message.Body()

	assert.Equal(t, 96, offset)
	assert.Equal(t, bytes[0], byte(7))
	assert.Equal(t, bytes[1], byte(Integer))
	assert.Equal(t, bytes[2], byte(Float))
	assert.Equal(t, bytes[3], byte(Boolean))
	assert.Equal(t, bytes[4], byte(Blob))
	assert.Equal(t, bytes[5], byte(Text))
	assert.Equal(t, bytes[6], byte(Null))
	assert.Equal(t, bytes[7], byte(ISO8601))
}

func TestMessage_putNamedValues32(t *testing.T) {
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

	message.putNamedValues32(values)

	bytes, offset := message.Body()

	assert.Equal(t, 104, offset)
	assert.Equal(t, bytes[0], byte(7))
	assert.Equal(t, bytes[1], byte(0))
	assert.Equal(t, bytes[2], byte(0))
	assert.Equal(t, bytes[3], byte(0))
	assert.Equal(t, bytes[4], byte(Integer))
	assert.Equal(t, bytes[5], byte(Float))
	assert.Equal(t, bytes[6], byte(Boolean))
	assert.Equal(t, bytes[7], byte(Blob))
	assert.Equal(t, bytes[8], byte(Text))
	assert.Equal(t, bytes[9], byte(Null))
	assert.Equal(t, bytes[10], byte(ISO8601))
}

func TestMessage_putHeader(t *testing.T) {
	message := Message{}
	message.Init(64)

	message.PutString("hello")
	message.putHeader(RequestExec, 1)
}

func BenchmarkMessage_putString(b *testing.B) {
	message := Message{}
	message.Init(4096)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		message.reset()
		message.PutString("hello")
	}
}

func BenchmarkMessage_putUint64(b *testing.B) {
	message := Message{}
	message.Init(4096)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		message.reset()
		message.PutUint64(270)
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

			message.PutString(c.String)
			message.putHeader(0, 0)

			message.Rewind()

			s := message.getString()

			_, offset := message.Body()

			assert.Equal(t, s, c.String)
			assert.Equal(t, offset, c.Offset)
		})
	}
}

func TestMessage_getBlob(t *testing.T) {
	cases := []struct {
		Blob   []byte
		Offset int
	}{
		{[]byte{1, 2, 3, 4, 5}, 16},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, 16},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 24},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%d", c.Offset), func(t *testing.T) {
			message := Message{}
			message.Init(64)

			message.PutBlob(c.Blob)
			message.putHeader(0, 0)

			message.Rewind()

			bytes := message.getBlob()

			_, offset := message.Body()

			assert.Equal(t, bytes, c.Blob)
			assert.Equal(t, offset, c.Offset)
		})
	}
}

func BenchmarkMessage_getBlob(b *testing.B) {
	makeBlob := func(size int) []byte {
		blob := make([]byte, size)
		for i := range blob {
			blob[i] = byte(i)
		}
		return blob
	}

	for _, size := range []int{16, 64, 256, 1024, 4096, 8096} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			message := Message{}
			message.Init(size + 16)

			message.PutBlob(makeBlob(size))
			message.putHeader(0, 0)

			for i := 0; i < b.N; i++ {
				message.Rewind()
				_ = message.getBlob()
			}
		})
	}
}

// The overflowing string ends exactly at word boundary.
func TestMessage_getString_Overflow_WordBoundary(t *testing.T) {
	message := Message{}
	message.Init(8)

	message.PutBlob([]byte{
		'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
		'i', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
		0, 0, 0, 0, 0, 0, 0,
	})
	message.putHeader(0, 0)

	message.Rewind()
	message.getUint64()

	s := message.getString()
	assert.Equal(t, "abcdefghilmnopqr", s)

	assert.Equal(t, 32, message.body.Offset)
}
