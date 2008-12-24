all:: test
SHELL = bash -e -o pipefail
O := $(shell echo $$$$)

T := $(wildcard test/test*.rb)
TO := $(subst .rb,.$(O).log, $(T))

test: $(TO)
	@cat $^ | ruby test/aggregate.rb
	@$(RM) $^
clean:
	$(RM) test/*.log test/*.log+

t = $(basename $(notdir $<))
%.$(O).log: %.rb
	@echo $(t); ruby -I lib $< $(TEST_OPTS) > $@+ 2>&1
	@mv $@+ $@
