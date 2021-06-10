/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.ndbench.plugin.configs;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;
//import java.util.List;
//import java.util.Arrays;

@Configuration(prefix =  NdBenchConstants.PROP_NAMESPACE +  "fenris1")
public interface CassandraFenris1Configuration extends CassandraConfigurationBase {
    @PropertyName(name = "rowsPerPartition")
    @DefaultValue("1")
    Integer getRowsPerPartition();

    @PropertyName(name = "colsPerRow")
    @DefaultValue("1")
    Integer getColsPerRow();

    @DefaultValue("false")
    Boolean getUseBatchWrites();

    @DefaultValue("false")
    Boolean getValidateRowsPerPartition();

//    @PropertyName(name = "cfname3")
//    @DefaultValue("test3")
//    String getCfname3();
//
//    @PropertyName(name = "cfname4")
//    @DefaultValue("test4")
//    String getCfname4();

//    @PropertyName(name = "hostList")
//    // Ignore PMD java rule as inapplicable because we're setting an overridable default
//    @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
//    // Default to running locally against local C* host
//    // May not be necessary beause the driver default is this value anyways
//    default List<String> getHostList() {
//        return Arrays.asList("127.0.0.1:9042");
//    }
}
