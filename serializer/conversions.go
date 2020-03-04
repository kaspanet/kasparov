package serializer

import "encoding/binary"

// BytesToUint64 converts the given byte array to uint64
func BytesToUint64(toConvert []byte) uint64 {
	return binary.LittleEndian.Uint64(toConvert)
}

// Uint64ToBytes converts the given uint64 array to a byte array
func Uint64ToBytes(toConvert uint64) []byte {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, toConvert)
	return result
}
