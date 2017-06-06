package com.template.mr.wc.flow.bean;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by huanghh on 2017/6/6.
 */
public class FlowBean implements WritableComparable<FlowBean>{
    private String phone;
    private long flow_up;
    private long flow_down;
    private long flow_sum;

    public FlowBean() {
    }

    public FlowBean(String phone, long flow_up, long flow_down) {
        setValue(phone, flow_up, flow_down);
    }

    private void setValue(String phone, long flow_up, long flow_down) {
        this.phone = phone;
        this.flow_up = flow_up;
        this.flow_down = flow_down;
        flow_sum = flow_up + flow_down;
    }

    public FlowBean(String line) {
        this(line,true);
    }

    public FlowBean(String line,boolean isRawData) {
        if(isRawData) {
            String[] words = StringUtils.split(line,'\t');
            phone = words[1];
            flow_up = Long.parseLong(words[7]);
            flow_down = Long.parseLong(words[8]);
        }else{
            String[] words = StringUtils.split(line,'\t');
            phone = words[0];
            flow_up = Long.parseLong(words[1]);
            flow_down = Long.parseLong(words[2]);
        }
        setValue(phone, flow_up, flow_down);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(flow_up);
        out.writeLong(flow_down);
        out.writeLong(flow_sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        phone = in.readUTF();
        flow_up = in.readLong();
        flow_down = in.readLong();
        flow_sum = in.readLong();
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Long getFlow_up() {
        return flow_up;
    }

    public void setFlow_up(Long flow_up) {
        this.flow_up = flow_up;
    }

    public Long getFlow_down() {
        return flow_down;
    }

    public void setFlow_down(Long flow_down) {
        this.flow_down = flow_down;
    }

    public Long getFlow_sum() {
        return flow_sum;
    }

    public void setFlow_sum(Long flow_sum) {
        this.flow_sum = flow_sum;
    }

    @Override
    public String toString() {
        return String.format("%s\t%s\t%s",flow_up,flow_down,flow_sum);
    }


    @Override
    public int compareTo(FlowBean o) {
        return flow_sum > o.flow_sum ? -1 :1;
    }
}
