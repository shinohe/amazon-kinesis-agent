/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file.
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.amazon.kinesis.streaming.agent.processing.processors;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;

/**
 * Replace sensitive data
 *
 * Configuration looks like:
 *
 * {
 *     "optionName": "JSONMASK",
 *     "pattern": "(D[1,5]00)[0-9a-zA-Z]*",
 *     "mask": "$0********"
 * }
 *
 * @author chaocheq
 *
 */
public class DataMaskConverter implements IDataConverter {

    private static String PATTERN_KEY = "pattern";
    private static String MASK_CHAR_KEY = "mask";
    private final String pattern;
    private final String maskStr;

    public DataMaskConverter(Configuration config) {
        pattern = config.readString(PATTERN_KEY, "(D[1,5]00)[0-9a-zA-Z]*");
        maskStr = config.readString(MASK_CHAR_KEY, "$1********");
    }

    @Override
    public ByteBuffer convert(ByteBuffer data) throws DataConversionException {
        String dataStr = ByteBuffers.toString(data, StandardCharsets.UTF_8);
        String rplaceStr = dataStr.replaceAll(pattern, maskStr);
        return ByteBuffer.wrap(rplaceStr.getBytes());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
