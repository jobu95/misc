.PHONY=scheduling_sim
scheduling_sim: scheduling_sim.c
	gcc -Wall -Wextra -O3 $< -o $@

.PHONY=clean
clean:
	rm -f scheduling_sim
