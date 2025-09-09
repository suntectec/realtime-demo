package com.sands.jp;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Builder
public class Student {

    private Integer id;
    private String name;

    @JSONField(name = "phone_number")
    private String phoneNumber;

    @JSONField(name = "age_years")
    private Integer ageYears;

}