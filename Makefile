all:
	nosetests

init:
	virtualenv --python=python3 env
	. env/bin/activate && pip install -r requirements.txt --use-mirrors
