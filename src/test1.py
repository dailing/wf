import time

import redis

r = redis.Redis('localhost', 6379)

print(r.lpush('aaa', 'a'))
print('sleep')
time.sleep(5)

print('expire')
p = r.pipeline()
p.lpush('aaa', 'end')
p.expire('aaa', 10)
print(p.execute())