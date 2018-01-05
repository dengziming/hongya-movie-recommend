package com.hongya.bigdata.algorithm;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.spark.deploy.yarn.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hongya.bigdata.run.SparkDeployUtils;

public class MonitorThread implements Runnable {
	private Logger log = LoggerFactory.getLogger(getClass());
	private ApplicationId appId;
	private Client client ;
	
	public MonitorThread() {
	}
	public MonitorThread(ApplicationId applicationId,Client client) {
		this.appId = applicationId;
		this.client = client;
	}
	@Override
	public void run() {
		long interval = 1000;// 更新Application 状态间隔
		int count =0; // 时间
		ApplicationReport report = null;
		while (true) {
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			try {
				report = client.getApplicationReport(appId);
			} catch (Exception e) {
				e.printStackTrace();
			}
			YarnApplicationState state = report.getYarnApplicationState();
			log.info("Thread:"+Thread.currentThread().getName()+
						appId.toString()+"，任务状态是："+state.name());
			// 完成/ 失败/杀死
			if (state == YarnApplicationState.FINISHED || state == YarnApplicationState.FAILED
					|| state == YarnApplicationState.KILLED) {
				SparkDeployUtils.cleanupStagingDir(appId);
				// return (state, report.getFinalApplicationStatus);
				//  更新 app状态
				log.info("Thread:"+Thread.currentThread().getName()+
						appId.toString()+"完成，任务状态是："+state.name());
				SparkDeployUtils.updateAppStatus(appId.toString(), state.name());
				return;
			}
			// 获得ApplicationID后就说明已经是SUBMITTED状态了
			if ( state == YarnApplicationState.ACCEPTED) {
		        //  更新app状态
				if(count<Integer.parseInt(SparkDeployUtils.getProperty("als.accepted.progress"))){
					count++;
					SparkDeployUtils.updateAppStatus(appId.toString(), count+"%" );
				}
		      }
			if ( state == YarnApplicationState.RUNNING) {
		        //  更新app状态
				if(count<Integer.parseInt(SparkDeployUtils.getProperty("als.runing.progress"))){
					count++;
					SparkDeployUtils.updateAppStatus(appId.toString(), count+"%" );
				}else {
					SparkDeployUtils.updateAppStatus(appId.toString(), SparkDeployUtils.getProperty("als.runing.progress")+"%" );
				}
		      }
		}
	}

}
