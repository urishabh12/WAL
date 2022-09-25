package file_manager

import "os"

func Read(path string) ([]byte, error) {
	fInfo, err := os.Stat(path)
	if err != nil {
		return []byte{}, err
	}

	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return []byte{}, err
	}
	defer f.Close()

	data := make([]byte, fInfo.Size())
	_, err = f.Read(data)
	if err != nil {
		return []byte{}, err
	}

	return data, nil
}

func Write(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return f.Sync()
}

func Append(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return f.Sync()
}

func OpenFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
}

func AppendToFile(f *os.File, data []byte) error {
	_, err := f.Write(data)
	return err
}

func SyncFile(f *os.File) error {
	return f.Sync()
}

func Delete(path string) error {
	return os.RemoveAll(path)
}
