package com.demo.practice.kcl.worker;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
/**
 * @author laxman-edara
 *
 */
public class TxnRecordProcessorFactory implements IRecordProcessorFactory {

	/**
	 * Constructor.
	 */
	public TxnRecordProcessorFactory() {
		super();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IRecordProcessor createProcessor() {
		return new TxnRecordProcess();
	}
}