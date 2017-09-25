package com.demo.practice.kcl.worker;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.model.Record;

public class TxnRecordProcess implements IRecordProcessor {

	private final static CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
	private static final long BACKOFF_TIME_IN_MILLIS = 3000;
	private static final int NUM_RETRIES = 5;
	private final static Logger LOGGER = Logger.getLogger(TxnRecordProcess.class.getName());

	
	public void initialize(InitializationInput initializationInput) {
		initializationInput
				.withExtendedSequenceNumber(new ExtendedSequenceNumber(SentinelCheckpoint.TRIM_HORIZON.toString()));

	}

	public void processRecords(ProcessRecordsInput processRecordsInput) {
		try {
			List<Record> records = processRecordsInput.getRecords();
			if (!records.isEmpty()) {
				records.forEach(record -> {
					String data;
					try {
						data = decoder.decode(record.getData()).toString();
						LOGGER.log(Level.INFO, data);
					} catch (Exception e) {
						e.printStackTrace();
					}

				});
				try {
					Thread.sleep(BACKOFF_TIME_IN_MILLIS);
					processRecordsInput.getCheckpointer().checkpoint();
				} catch (InterruptedException | ShutdownException | InvalidStateException e) {
					e.printStackTrace();
				}

			}
		} catch (KinesisClientLibDependencyException | ThrottlingException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void shutdown(ShutdownInput shutdownInput) {
		System.out.println("in the shutdown method...");
		if (shutdownInput.getShutdownReason().equals(ShutdownReason.TERMINATE)) {
			checkpoint(shutdownInput.getCheckpointer());
		}

	}

	private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
		for (int i = 0; i < NUM_RETRIES; i++) {
			try {
				checkpointer.checkpoint();
				break;
			} catch (ShutdownException | KinesisClientLibDependencyException | InvalidStateException e) {
				LOGGER.log(Level.INFO, "skipping checkpoint.", e);
				break;
			} catch (ThrottlingException e) {
				if (i >= (NUM_RETRIES - 1)) {
					LOGGER.log(Level.FINER, "Checkpoint failed after " + (i + 1) + "attempts.", e);
					break;
				} else {
					LOGGER.log(Level.INFO, "Transient issue when checkpointing - attempt " + (i + 1) + " of "
							+ NUM_RETRIES);
				}
			}
			try {
				Thread.sleep(BACKOFF_TIME_IN_MILLIS);
			} catch (InterruptedException e) {
				LOGGER.log(Level.INFO, "Interrupted sleep", e);
			}

		}
	}

}
