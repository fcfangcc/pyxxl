# 常见问题与错误

## 调度任务成功，但回调时会重定向到登录

可能是xxl-job-admin服务的配置取消了上下文信息，导致默认的api接口变了Example的地址不可用

可以根据排查 [issues52](https://github.com/fcfangcc/pyxxl/issues/52)

## 报错The access token is wrong

xxl-job-admin服务配置了token验证，按配置中说明的配置正确的token即可
