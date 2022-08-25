package com.example.task;

/**
 * @author zhanghailin
 * @date 2022/7/5
 */
public enum YmmMapReduceMode {
    DEFAULT(1, "do as default mode"),
    MAP_REDUCE(2,"do as mapreduce mode");

    YmmMapReduceMode(Integer code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    private Integer code;
    private String msg;

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public static YmmMapReduceMode getMode(Integer code) {
        for (YmmMapReduceMode value : YmmMapReduceMode.values()) {
            if (value.getCode().equals(code)) {
                return value;
            }
        }
        return DEFAULT;
    }
}
