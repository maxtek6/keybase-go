# Copyright (c) 2024 Maxtek Consulting
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

UNIT_TEST_HEADER        = "****************************** UNIT TEST *******************************"
LINT_CHECK_HEADER       = "***************************** LINT CHECK *******************************"
CODE_COVERAGE_HEADER    = "**************************** CODE COVERAGE *****************************" 

.PHONY: all
all: lint test

.PHONY: test
test: unit cover

.PHONY: unit
unit:
	@echo $(UNIT_TEST_HEADER)
	go test -v -timeout 30s -coverprofile=coverage.out ./...

.PHONY: cover
cover:
	@echo $(CODE_COVERAGE_HEADER)
	go tool cover -func=coverage.out

.PHONY: lint
lint:
	@echo $(LINT_TEST_HEADER)
	@if [ ! -f bin/golangci-lint ]; then \
    	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b bin; \
	fi
	./bin/golangci-lint -v run ./...

.PHONY: clean
clean:
	rm -rf coverage.out