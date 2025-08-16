package com.atguigu.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: TableProcess
 * Package: com.spdbccc.bean
 * Description:
 *
 * @Author JWT
 * @Create 2025/8/5 17:10
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcess {
    String sourceTable;
    String sourceType;
    String sinkTable;
    String sinkType;
    String sinkColumns;

}
