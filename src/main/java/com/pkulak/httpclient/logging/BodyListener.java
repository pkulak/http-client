package com.pkulak.httpclient.logging;

import org.asynchttpclient.HttpResponseBodyPart;

public interface BodyListener {
    void onBodyPartReceived(HttpResponseBodyPart bodyPart);

    void onRequestComplete();
}
