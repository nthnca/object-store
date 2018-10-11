// Code generated by MockGen. DO NOT EDIT.
// Source: interfaces.go

// Package mock_objectstore is a generated GoMock package.
package objectstore

import (
	context "context"
	"fmt"
	"log"
	reflect "reflect"
	"testing"

	"cloud.google.com/go/storage"
	gomock "github.com/golang/mock/gomock"
	"google.golang.org/api/iterator"
)

// MockstorageClientInterface is a mock of storageClientInterface interface
type MockstorageClientInterface struct {
	ctrl     *gomock.Controller
	recorder *MockstorageClientInterfaceMockRecorder
}

// MockstorageClientInterfaceMockRecorder is the mock recorder for MockstorageClientInterface
type MockstorageClientInterfaceMockRecorder struct {
	mock *MockstorageClientInterface
}

// NewMockstorageClientInterface creates a new mock instance
func NewMockstorageClientInterface(ctrl *gomock.Controller) *MockstorageClientInterface {
	mock := &MockstorageClientInterface{ctrl: ctrl}
	mock.recorder = &MockstorageClientInterfaceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockstorageClientInterface) EXPECT() *MockstorageClientInterfaceMockRecorder {
	return m.recorder
}

// list mocks base method
func (m *MockstorageClientInterface) list(ctx context.Context) storageListInterface {
	ret := m.ctrl.Call(m, "list", ctx)
	ret0, _ := ret[0].(storageListInterface)
	return ret0
}

// list indicates an expected call of list
func (mr *MockstorageClientInterfaceMockRecorder) list(ctx interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "list", reflect.TypeOf((*MockstorageClientInterface)(nil).list), ctx)
}

// readFile mocks base method
func (m *MockstorageClientInterface) readFile(ctx context.Context, filename string) ([]byte, error) {
	ret := m.ctrl.Call(m, "readFile", ctx, filename)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// readFile indicates an expected call of readFile
func (mr *MockstorageClientInterfaceMockRecorder) readFile(ctx, filename interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "readFile", reflect.TypeOf((*MockstorageClientInterface)(nil).readFile), ctx, filename)
}

// writeFile mocks base method
func (m *MockstorageClientInterface) writeFile(ctx context.Context, filename string, contents []byte) error {
	ret := m.ctrl.Call(m, "writeFile", ctx, filename, contents)
	ret0, _ := ret[0].(error)
	return ret0
}

// writeFile indicates an expected call of writeFile
func (mr *MockstorageClientInterfaceMockRecorder) writeFile(ctx, filename, contents interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "writeFile", reflect.TypeOf((*MockstorageClientInterface)(nil).writeFile), ctx, filename, contents)
}

// deleteFile mocks base method
func (m *MockstorageClientInterface) deleteFile(ctx context.Context, filename string) error {
	ret := m.ctrl.Call(m, "deleteFile", ctx, filename)
	ret0, _ := ret[0].(error)
	return ret0
}

// deleteFile indicates an expected call of deleteFile
func (mr *MockstorageClientInterfaceMockRecorder) deleteFile(ctx, filename interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "deleteFile", reflect.TypeOf((*MockstorageClientInterface)(nil).deleteFile), ctx, filename)
}

// MockstorageListInterface is a mock of storageListInterface interface
type MockstorageListInterface struct {
	ctrl     *gomock.Controller
	recorder *MockstorageListInterfaceMockRecorder
}

// MockstorageListInterfaceMockRecorder is the mock recorder for MockstorageListInterface
type MockstorageListInterfaceMockRecorder struct {
	mock *MockstorageListInterface
}

// NewMockstorageListInterface creates a new mock instance
func NewMockstorageListInterface(ctrl *gomock.Controller) *MockstorageListInterface {
	mock := &MockstorageListInterface{ctrl: ctrl}
	mock.recorder = &MockstorageListInterfaceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockstorageListInterface) EXPECT() *MockstorageListInterfaceMockRecorder {
	return m.recorder
}

// next mocks base method
func (m *MockstorageListInterface) next() (string, error) {
	ret := m.ctrl.Call(m, "next")
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// next indicates an expected call of next
func (mr *MockstorageListInterfaceMockRecorder) next() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "next", reflect.TypeOf((*MockstorageListInterface)(nil).next))
}

func ErrorPrefix(err error, prefix string) {
	if err.Error()[:len(prefix)] != prefix {
		log.Fatalf("Unexpected error: %v", err)
	}
}

type baseFunc func()

func TestErrors(t *testing.T) {
	mockClient := NewMockstorageClientInterface(nil)
	mockList := NewMockstorageListInterface(nil)
	curr := int64(1)
	unixNano = func() int64 {
		curr += 1
		return curr
	}

	tests := []func(){
		func() {
			log.Printf("List Error")

			mockClient.EXPECT().list(nil).Return(mockList)
			mockList.EXPECT().next().Return("", fmt.Errorf("Sorry"))

			_, err := internal_new(nil, mockClient)
			ErrorPrefix(err, "Failed to iterate through objects: ")
		},
		func() {
			log.Printf("List Error Oops")

			mockClient.EXPECT().list(nil).Return(mockList)
			mockList.EXPECT().next().Return("abc", nil)
			mockList.EXPECT().next().Return("", fmt.Errorf("Sorry"))
			b := []byte{10, 8, 10, 1, 107, 16, 2, 26, 1, 97}
			mockClient.EXPECT().readFile(nil, "abc").Return(b, nil)

			_, err := internal_new(nil, mockClient)
			ErrorPrefix(err, "Failed to iterate through objects: ")
		},
		func() {
			log.Printf("Read Error")

			mockClient.EXPECT().list(nil).Return(mockList)
			mockList.EXPECT().next().Return("abc", nil)
			mockList.EXPECT().next().Return("", iterator.Done)
			mockClient.EXPECT().readFile(nil, "abc").
				Return(nil, fmt.Errorf("Sorry"))

			_, err := internal_new(nil, mockClient)
			ErrorPrefix(err, "Reading file: ")
		},
		func() {
			log.Printf("Read File Not Found")

			mockClient.EXPECT().list(nil).Return(mockList)
			mockList.EXPECT().next().Return("abc", nil)
			mockList.EXPECT().next().Return("", iterator.Done)
			mockClient.EXPECT().readFile(nil, "abc").
				Return(nil, storage.ErrObjectNotExist)

			_, err := internal_new(nil, mockClient)
			if err != storage.ErrObjectNotExist {
				log.Fatalf("Unexpected error: %v", err)
			}
		},
		func() {
			log.Printf("Success")
			curr = 1

			mockClient.EXPECT().list(nil).Return(mockList)
			mockList.EXPECT().next().Return("", iterator.Done)
			mockClient.EXPECT().writeFile(nil,
				"3a08adf873974f4578dfc4be2d1ac4cc04b6e9e7db78aa8761c309189c35c721.os",
				[]byte{10, 8, 10, 1, 107, 16, 2, 26, 1, 97}).Return(nil)

			os, err := internal_new(nil, mockClient)
			err = os.Insert(nil, "k", []byte("a"))
			if err != nil {
				log.Fatalf("Expected success: %v", err)
			}
		},
	}

	for _, f := range tests {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()

		mockClient = NewMockstorageClientInterface(mockCtrl)
		mockList = NewMockstorageListInterface(mockCtrl)

		f()
	}
}
