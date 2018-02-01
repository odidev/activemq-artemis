/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.cli.commands.tools;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.paging.impl.PageTransactionInfoImpl;

public class PageTransactionCleanup extends HousekeepPlugin {
   @Override
   public void onMessageJournal(Journal journal, List<PreparedTransactionInfo> preparedTransactions, List<RecordInfo> records) throws Exception {
      System.out.println("Treating " + records.size() + " records on PageTransactionCleanup");
      long size = records.size();
      long count = 0;
      boolean sync = false;
      for (RecordInfo record : records) {
         count++;
         if (count % 5000 == 0) {
            sync = true;
            double percentage = (count / (double) size) * 100f;
            System.out.println("Processed " + count + " of " + size + " " + percentage + "%");
         }
         Object object = decode(record);
         if (object instanceof PageTransactionInfoImpl) {
            PageTransactionInfoImpl pgtx = (PageTransactionInfoImpl) object;
            journal.appendDeleteRecord(pgtx.getRecordID(), sync);
            sync = false;
         }
      }

      journal.scheduleCompactAndBlock(3600);
   }

   @Override
   public void oneBindingsJournal(Journal journal, List<PreparedTransactionInfo> preparedTransactions, List<RecordInfo> records) throws Exception {
   }
}
