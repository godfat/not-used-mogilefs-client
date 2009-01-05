all:: test

T := $(wildcard test/test*.rb)
TO := $(subst .rb,.log,$(T))

test: $(T)
	@cat $(TO) | ruby test/aggregate.rb
	@$(RM) $(TO)
clean:
	$(RM) $(TO) $(addsuffix +,$(TO))

t = $(basename $(notdir $@))
t_log = $(subst .rb,.log,$@)

$(T):
	@echo $(t); ruby -I lib $@ $(TEST_OPTS) > $(t_log)+ 2>&1
	@mv $(t_log)+ $(t_log)

Manifest.txt:
	git ls-files > $@+
	mv $@+ $@

.PHONY: $(T) Manifest.txt
