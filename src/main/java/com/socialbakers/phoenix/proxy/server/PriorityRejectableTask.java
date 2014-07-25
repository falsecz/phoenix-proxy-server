package com.socialbakers.phoenix.proxy.server;

public interface PriorityRejectableTask {

	int getPriority();

	void reject();

}
