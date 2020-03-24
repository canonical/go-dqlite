package protocol

func (m *Message) Body() ([]byte, int) {
	return m.body.Bytes, m.body.Offset
}

func (m *Message) Rewind() {
	m.body.Offset = 0
}
