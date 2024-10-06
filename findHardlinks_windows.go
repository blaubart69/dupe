package main

import (
	"syscall"
	"unicode/utf16"
	"unsafe"
)

var (
	kernel32DLL        = syscall.NewLazyDLL("kernel32.dll")
	FindFirstFileNameW = kernel32DLL.NewProc("FindFirstFileNameW")
	FindNextFileNameW  = kernel32DLL.NewProc("FindNextFileNameW")
)

/*
int wmain(int argc, wchar_t* argv[])
{
    if (argc != 2) {
        wprintf(L"usage: %s {filename}\n", argv[0]);
        return 99;
    }

    WCHAR   buf[1024];
    DWORD   len = 1024;

    HANDLE hFind = FindFirstFileNameW(argv[1], 0, &len, buf);
    if (hFind == INVALID_HANDLE_VALUE) {
        PrintLastError(L"FindFirstFileNameW");
        return GetLastError();
    }

    for (;;) {
        wprintf(L"%s\n", buf);

        if (!FindNextFileNameW(hFind, &len, buf)) {
            if (GetLastError() == ERROR_HANDLE_EOF) {
                break;
            }
            else {
                PrintLastError(L"FindNextFileNameW");
                return GetLastError();
            }
        }
    }
    return 0;
}
*/

func FindFirst(filename string, buf *[]uint16) (syscall.Handle, string, error) {

	len := len(*buf)
	ptrBuf := &((*buf)[0])

	utf16Filename := StringToUTF16Ptr(filename)

	for {
		hFind, _, lastErr := FindFirstFileNameW.Call(
			uintptr(unsafe.Pointer(utf16Filename)),
			uintptr(uint32(0)),
			uintptr(unsafe.Pointer(&len)),
			uintptr(unsafe.Pointer(ptrBuf)))

		if syscall.Errno(syscall.InvalidHandle) == syscall.Errno(hFind) {
			if syscall.ERROR_MORE_DATA == lastErr {
				*buf = make([]uint16, int(len))
				ptrBuf = &((*buf)[0])
				continue
			} else {
				return syscall.InvalidHandle, "", lastErr
			}
		} else {
			return syscall.Handle(hFind), syscall.UTF16ToString((*buf)[:len]), nil
		}
	}
}

func FindNext(hFind syscall.Handle, buf *[]uint16) (string, error) {
	len := len(*buf)
	ptrBuf := &((*buf)[0])

	for {
		r1, _, lastErr := FindNextFileNameW.Call(
			uintptr(hFind),
			uintptr(unsafe.Pointer(&len)),
			uintptr(unsafe.Pointer(ptrBuf)))

		if r1 == 0 {
			if syscall.ERROR_HANDLE_EOF == lastErr {
				return "", nil
			} else if syscall.ERROR_MORE_DATA == lastErr {
				*buf = make([]uint16, int(len))
				ptrBuf = &((*buf)[0])
				continue
			} else {
				return "", lastErr
			}
		} else {
			return syscall.UTF16ToString((*buf)[:len]), nil
		}
	}
}

func findHardlinks(filename string, buf *[]uint16) ([]string, error) {

	hFind, hardlinkname, err := FindFirst(filename, buf)
	if err != nil {
		return nil, err
	}

	filenames := append(make([]string, 0), hardlinkname)

	for {
		hardlinkname, err = FindNext(hFind, buf)
		if err != nil {
			return nil, err
		} else if hardlinkname == "" { // EOF
			break
		} else {
			filenames = append(filenames, hardlinkname)
		}
	}

	return filenames, nil
}

// StringToUTF16Ptr converts a Go string into a pointer to a null-terminated UTF-16 wide string.
// This assumes str is of a UTF-8 compatible encoding so that it can be re-encoded as UTF-16.
func StringToUTF16Ptr(str string) *uint16 {
	wchars := utf16.Encode([]rune(str + "\x00"))
	return &wchars[0]
}
