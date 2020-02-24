package serializer

import "encoding/binary"

func BytesToUint64(toConvert []byte) uint64 {
	return binary.LittleEndian.Uint64(toConvert)
}

func Uint64ToBytes(toConvert uint64) []byte {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, toConvert)
	return result
}
