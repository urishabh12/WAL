package file_reader

import (
	"fmt"
	"testing"
)

func Test_ReadAndWrite(t *testing.T) {
	path := "test.txt"
	writeText := "Hakuna Matata"
	defer Delete(path)
	err := Write(path, []byte(writeText))
	handleErr(err, t)
	data, err := Read(path)
	handleErr(err, t)
	assertTrueString(string(data), writeText, t)
}

func Test_Append(t *testing.T) {
	path := "test.txt"
	writeText := "Hakuna Matata"
	readTest := "Hakuna MatataHakuna Matata"
	defer Delete(path)
	err := Append(path, []byte(writeText))
	handleErr(err, t)
	err = Append(path, []byte(writeText))
	handleErr(err, t)
	data, err := Read(path)
	handleErr(err, t)
	assertTrueString(string(data), readTest, t)
}

func Test_ReadNonExistentFile(t *testing.T) {
	path := "test.txt"
	_, err := Read(path)
	handleNotErr(err, t)
}

func handleErr(err error, t *testing.T) {
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func handleNotErr(err error, t *testing.T) {
	if err == nil {
		t.Fatalf(err.Error())
	}
}

func assertTrueString(a string, b string, t *testing.T) {
	if a != b {
		t.Fatalf(fmt.Sprintf("%s is not equal to %s", a, b))
	}
}
