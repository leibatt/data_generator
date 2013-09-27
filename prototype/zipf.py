import math
import random
import numpy as np

c = 0.0
first = True

c2 = 0.0
carr = None

def zipf(alpha,n):
  global c
  global first

  if first:
    c = 0.0
    for i in range(1,n+1):
      c += 1.0 / math.pow(i,alpha)
    c = 1.0 / c
    first = False

  z = random.random()
  while z == 0.0: # random excludes 1.0 by default
    z = random.random()

  sum_prob = 0
  zipf_value = 0
  for i in range(1,n+1):
    sum_prob += c / math.pow(i,alpha)
    if sum_prob >= z:
      zipf_value = i
      break

  if (zipf_value < 1) or (zipf_value > n):
    raise Exception("Zipf value out of bounds")

  return zipf_value

def zipf_variable(alpha,n):
  return lambda: zipf(alpha,n)

#broken
def zipf2(alpha,n):
  global carr
  global c2
  if carr is None:
    print "carr is None, calculating..."
    carr = np.zeros(n)
    temp = np.array(range(1,n+1)).astype(float)
    temp = np.power(temp,alpha)
    temp = 1.0 / temp
    c2 = 1.0 / np.sum(temp)
    temp = c2 / temp
    carr[0] = temp[0]
    for i in range(1,n):
      if (i % 100000) == 0:
         print "computing step",i
      carr[i] = carr[i-1] + temp[i]
    print "done calculating carr"

  z = np.random.uniform(.0000001,1)
  for i in range(1,n+1):
    if carr[i] >= z:
      return i
      
