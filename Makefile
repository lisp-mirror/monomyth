##
# Monomyth
#
# @file

build-test-image:
	docker build . --tag smallerinfinity/monomyth-ci

test:
	qlot exec ./bin/test.ros

# end
