# Immich 反向地理编码汉化

这是一个为 Immich 的反向地理位编码功能提供汉化支持的项目，主要解决 Immich 识别照片拍摄位置都是英文的问题，并对结果进行了优化

- **地点中文化**：对国内的地址（含港澳台）实现了完整汉化，并实验性支持部分海外地区（目前包括日本）
- **地址标准化**：通过高德/Nominatim API，将地址统一规范为 **一级**、**二级**、**三级**、**四级行政区** 四个层级，避免 Immich 默认使用的非标准或冷僻地名，且具体粒度可选。
- **自动更新**：仓库会每周定期拉取最新地理信息数据，并自动发布更新版本。

以下是使用后的前后对比

![](./image/example.png)

# 如何使用

### 1. 下载数据
在 [Release 页面](https://github.com/ZingLix/immich-geodata-cn/releases/latest) 下载 geodata.zip 和 i18n-iso-countries.zip 两个文件并解压

数据发布分为以下两类：  
- 🔄 [**自动更新版**](https://github.com/ZingLix/immich-geodata-cn/releases/tag/auto-release)：更新更频繁，推荐使用。但由于数据源可能发生变化，偶尔会导致生成的文件不可用。如遇问题可切换至手动发布版并提交 issue。  
- 🛠️ [**手动发布版**](https://github.com/ZingLix/immich-geodata-cn/releases)：相对稳定，适合作为备用选项。

文件名以 `_full` 结尾的版本数据量更大，在边界位置识别上表现更好，但可能会降低识别速度。  

<details>

<summary>`geodata` 文件的不同后缀代表支持的行政区划精度（如三级、四级），点击查看示例或参考 Release 页面</summary>

以 `中国,江苏省,苏州市,昆山市,周市镇` 为例

|示例|对应文件|数据增强<br>数据更多、定位更准但编码更慢|
|:---|:---|:---|
|苏州市|geodata.zip<br>geodata_admin_2.zip|geodata_full.zip<br>geodata_admin_2_full.zip|
|昆山市|geodata_admin_3.zip|geodata_admin_3_full.zip|
|周市镇|geodata_admin_4.zip|geodata_admin_4_full.zip|
|苏州市 昆山市|geodata_admin_2_admin_3.zip|geodata_admin_2_admin_3_full.zip|
|苏州市 周市镇|geodata_admin_2_admin_4.zip|geodata_admin_2_admin_4_full.zip|
|昆山市 周市镇|geodata_admin_3_admin_4.zip|geodata_admin_3_admin_4_full.zip|
|苏州市 昆山市 周市镇|geodata_admin_2_admin_3_admin_4.zip|geodata_admin_2_admin_3_admin_4_full.zip|

</details>

### 2. 配置 Docker 容器映射

修改你的 docker-compose.yaml，在 volumes 中添加以下挂载（或按你的部署方式替换目标文件夹）：

如果使用的是官方 `immich-app/immich-server` 镜像，修改路径如下

```yaml
# immich 版本 >= 1.136.0
volumes:
  - ./geodata:/build/geodata
  - ./i18n-iso-countries/langs:/usr/src/app/server/node_modules/i18n-iso-countries/langs

# immich 版本 < 1.136.0 (不含)
volumes:
  - ./geodata:/build/geodata
  - ./i18n-iso-countries/langs:/usr/src/app/node_modules/i18n-iso-countries/langs
```

如果使用的是 `imagegenius/immich` 镜像，修改路径如下（感谢 huazhaozhe [#18](https://github.com/ZingLix/immich-geodata-cn/discussions/18)）

```
volumes:
  - ./geodata:/app/immich/server/geodata
  - ./i18n-iso-countries/langs:/app/immich/server/node_modules/i18n-iso-countries/langs
```

### 3. 重启 Immich

运行 `docker compose down && docker compose up` 重启 Immich
  - 启动日志中若出现类似 10000 geodata records imported，说明 geodata 已成功更新。
  - 若无此日志，可尝试修改 geodata/geodata-date.txt 中的日期为当前时间，Immich 只会在文件日期新于上次加载时更新数据。
    ![](./image/importlog.jpg)

### 4. 刷新元数据

> **注意**：仅在首次使用本项目提供的地理数据时需要执行此步骤，用于更新所有已有照片的位置。  
> 后续新增照片无需重复此操作。

进入「系统管理 - 任务」页面，点击「提取元数据」中的「全部」，触发对所有照片位置信息的刷新。任务完成后，所有照片的位置将显示为中文地名并支持中文搜索。

# 如何更新

重新下载最新 Release 页面中的文件，替换掉原有文件，然后重启 Immich 即可完成更新。

为了方便快速更新，可以使用以下脚本自动下载并替换文件：

```bash
# cd geodata  在 geodata 目录下进行操作

# 下载脚本
curl -o update.sh https://raw.githubusercontent.com/ZingLix/immich-geodata-cn/refs/heads/main/geodata/update.sh

# 运行更新脚本，参数为你需要的版本，例如 geodata_admin_2
bash update.sh geodata_admin_2
```

更新完成后，需要重启 Immich 以重新加载数据。如果希望自动重启，可以在脚本最后增加类似 `docker restart immich_server` 的命令进行重启，或者通过类似如下定时任务的方式一起运行重启。

```bash
5 5 * * 6 bash /immich_data/geodata/update.sh && docker restart immich_server
```

# 如何生成数据

关于如何运作的，或者是想要自定义数据的，可以到 [此处](https://github.com/ZingLix/immich-geodata-cn/tree/main/geodata) 查看。

# License

GPL

# 💖 支持我

本项目承诺将 **永久免费开放使用**，如果你觉得它对你有帮助，欢迎请我喝杯咖啡 ☕，让这个项目可以持续维护下去并增加更多功能！

<img src="image/sponsor.jpg" alt="赞助" width="400" />
