package transactions

type SerializedContext struct {
}

func (c *SerializedContext) EncodeAsString() string {
	return ""
}

func (c *SerializedContext) EncodeAsBytes() []byte {
	return []byte{}
}
