package com.example.task;

/**
 * @author zhanghailin
 * @date 2022/7/5
 */
public enum YmmMapReduceErrorCode {
    ERROR_CODE_TIMEOUT(1, "ERROR_CODE_TIMEOUT"),
    ERROR_CODE_INTERRUPTED(2,"ERROR_CODE_INTERRUPTED"),
    ERROR_CODE_SUBTASK_FAILED(3,"ERROR_CODE_SUBTASK_FAILED");

    YmmMapReduceErrorCode(Integer code, String msg) {
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
}
