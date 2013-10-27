MODULE_NAME = fb_psql

install:
	@pip install .

uninstall:
	@pip uninstall $(MODULE_NAME)

clean:
	@rm -f *.pyc
