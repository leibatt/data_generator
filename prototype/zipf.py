import math
import random

def zipf(alpha,n):
  c = 0.0
  for i in range(1,n+1):
    c += 1.0 / math.pow(i,alpha)
  c = 1.0 / c

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
