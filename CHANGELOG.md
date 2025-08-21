### 0.4.4
* 增加retry相关的几个参数，用于控制请求xxl-admin时的重试配置

### 0.4.2
* 增加参数log_clean_interval
* 删除本地日志时释放cpu权限，防止卡死loop

### 0.4.1
* 修复配置中host和port的优先级 #68

### 0.4.0
* 配置移除 **executor_host** **executor_port** 使用 **executor_url** 替代
* 配置新增 **executor_logger**, 用于传入自定义logger[目前只能覆盖executor的日志]

### 0.3.4
* **executor_server_host** 修改为 **executor_listen_host**
* 新增配置 **executor_listen_port**

### 0.3.3
* 兼容XXL-JOB 2.2版本

### 0.1.7

* 优化错误信息
* 支持自定义执行器参数
* 支持executorTimeout
