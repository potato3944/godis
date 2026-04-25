package utils

import "hash/crc32"

func KeyHashSlot(key string) uint {
	return uint(crc32.ChecksumIEEE([]byte(key))) & 0x3FFF
}
