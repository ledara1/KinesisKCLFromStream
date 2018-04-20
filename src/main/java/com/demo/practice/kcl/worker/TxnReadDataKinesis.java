package com.demo.practice.kcl.worker;

import java.net.InetAddress;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class TxnReadDataKinesis {
	//This application name is created as a table in amazon dynamo DB
	private static final String APPLICATION_NAME = "TxnKCLKPL";
	//This is the stream name created in the Kinesis
	private static final String STREAM_NAME = "your kinesis stream name";

	public static void main(String[] args) throws Exception {
		String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
		KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(APPLICATION_NAME, STREAM_NAME,
				new ProfileCredentialsProvider(), workerId);
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		Date todate1 = cal.getTime();
		config.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
		config.withCallProcessRecordsEvenForEmptyRecordList(false).withTimestampAtInitialPositionInStream(todate1);
		IRecordProcessorFactory recordProcessorFactory = new TxnRecordProcessorFactory();
		Worker worker = new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(config).build();
		worker.run();

	}
}
