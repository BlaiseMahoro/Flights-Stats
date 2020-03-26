we computed the average 
ight per day from the
dataset, ontime 
ights.tsv. In this lab we will calculate the sample size, total
arrival delay, mean arrival delay, standard deviation, and the range. We will
use job chaining to do this.
Here is an example of the general approach to follow:
* the rst map, for each input line, should output the airport code as the
key, a value of 1 and the delay
* the rst reduce, will sum up the values of 1s and the delays, then calculates
the average delay and the range(min and max)
*  for each input that the reducer iterates with, it should output that input
with the new calculated values from the previous step
* *e second map should add to the previous list values the value of the
delay minus the average squared
* the second reducer should add up the new values that were included by
the second mapper and then calculates the standard deviation. It should
also output the nal results.