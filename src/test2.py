import redis


r = redis.Redis('localhost', 6379)

rr = r.ttl('aaa')
print(rr)
if rr == -1:
    print(r.brpop('aaa'))
    print(r.brpop('aaa'))
else:
    print('good now')