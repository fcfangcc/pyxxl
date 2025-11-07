# 常见问题与错误

## 请求xxl-admin被重定向到登录

可能是xxl-job-admin服务的配置取消了上下文信息，导致默认的api接口变了Example的地址不可用

可以根据 [issues52](https://github.com/fcfangcc/pyxxl/issues/52) 排查

错误信息可能包含类似的关键字

```python
File "/lib/python3.12/site-packages/aiohttp/client_reqrep.py", line 1199, in json
raise ContentTypeError(
aiohttp.client_exceptions.ContentTypeError: 0,
message='Attempt to decode JSON with unexpected mimetype: text/html;charset=utf-8', url='http://xxxxx/toLogin'
```

## 报错The access token is wrong

xxl-job-admin服务配置了token验证，按配置中说明的配置正确的token即可
