# use GNU Make to run tests in parallel, and without depending on Rubygems
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

# using make instead of rake since Rakefile takes too long to load
Manifest.txt:
	git ls-files > $@+
	mv $@+ $@

libs := $(wildcard lib/*.rb lib/*/*.rb)
flay_flags =
flog_flags =
flay: $(libs)
	flay $(flay_flags) $^
flog: $(libs)
	flog $(flog_flags) $^
.PHONY: $(T) Manifest.txt
