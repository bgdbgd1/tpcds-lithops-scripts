

BUFSIZE = 64 * 1024
def copyfileobj(fsrc, fdst):
	# shutil.copyfileobj creates a lot of garbage creating multiple buffers. This doesn't
	buf = bytearray(BUFSIZE)
	bytes_read = fsrc.readinto(buf)
	while bytes_read != 0:
		fdst.write(memoryview(buf)[:bytes_read])
		bytes_read = fsrc.readinto(buf)
