package rados

import (
	"github.com/stretchr/testify/assert"
)

func (suite *RadosTestSuite) TestSnapshots() {
	suite.SetupConnection()

	// Create some data prior to the snapshot
	err := suite.ioctx.Write("obj42", []byte("initial input data"), 0)
	assert.NoError(suite.T(), err)

	err = suite.ioctx.Write("obj96", []byte("initial input data"), 0)
	assert.NoError(suite.T(), err)

	// Create the snapshot
	err = suite.ioctx.SnapCreate("test")
	assert.NoError(suite.T(), err)

	sid, err := suite.ioctx.SnapLookup("test")
	assert.NoError(suite.T(), err)

	sname, err := suite.ioctx.SnapGetName(sid)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "test", sname)

	stime, err := suite.ioctx.SnapGetStamp(sid)
	assert.NoError(suite.T(), err)
	assert.NotZero(suite.T(), stime)

	// Add data after the snapshot
	err = suite.ioctx.Append("obj42", []byte(" additional data appended"))
	assert.NoError(suite.T(), err)

	err = suite.ioctx.Write("obj74", []byte("data written after the snapshot"), 0)
	assert.NoError(suite.T(), err)

	// Create a second snapshot
	err = suite.ioctx.SnapCreate("bravo")
	assert.NoError(suite.T(), err)

	snaps, err := suite.ioctx.SnapList(0)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), snaps, 2)
	assert.Equal(suite.T(), sid, snaps[0])

	// Test reading from the snapshot
	suite.ioctx.SetSnapRead(sid)

	data := make([]byte, 43)
	b, err := suite.ioctx.Read("obj42", data, 0)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 18, b)

	// Go back to reading normally
	suite.ioctx.SetSnapRead(SnapHead)

	data = make([]byte, 43)
	_, err = suite.ioctx.Read("obj42", data, 0)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "initial input data additional data appended", string(data))

	// Delete the snapshots
	err = suite.ioctx.SnapRemove("test")
	assert.NoError(suite.T(), err)

	err = suite.ioctx.SnapRemove("bravo")
	assert.NoError(suite.T(), err)

	snaps, err = suite.ioctx.SnapList(0)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), snaps, 0)
}
