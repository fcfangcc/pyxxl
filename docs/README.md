### Deploy new version

```
mike deploy -u v0.4.0 latest
mike deploy -u v0.4.0a1 dev
```

###
如果部署之后发现文档没有更新，可能需要重新安装下本地的pyxxl版本
```
pip install -e .
```

### run serve
```
mkdocs serve
```
