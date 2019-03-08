在现有的解析工具上修改，增加对表、字段级别血缘的识别度。
结合业务，开放对外接口。
对外接口入口：SqlParserController.java
Spark解析入口：ParseDriver.scala -> parsePlan
Hive解析入口：LineParse.java -> parse