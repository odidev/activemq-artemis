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

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.paging.impl.PageTransactionInfoImpl;
import org.apache.activemq.artemis.core.persistence.impl.journal.BatchingIDGenerator;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PageUpdateTXEncoding;

public class PageTransactionDirty extends HousekeepPlugin {

   long max = 16749311;
   @Override
   public void onMessageJournal(Journal journal, List<PreparedTransactionInfo> preparedTransactions, List<RecordInfo> records) throws Exception {
      System.out.println("&&&&& Treating " + records.size() + " records on PageTransactionCleanup");
      long recordId = 1;
      boolean sync = false;
      for (long i = 1; i < max; i++) {

         if (i % 5000 == 0) {
            System.out.println("total " + i + " of " + max);
            System.out.println("percentage = " + (i/(double)max) * 100);
            sync = true;
         }
         long tx = recordId ++;
         long id = recordId++;
         PageTransactionInfoImpl txInfo = new PageTransactionInfoImpl(tx);
         journal.appendAddRecordTransactional(tx, id, JournalRecordIds.PAGE_TRANSACTION, txInfo);
         journal.appendUpdateRecordTransactional(tx, id, JournalRecordIds.PAGE_TRANSACTION, new PageUpdateTXEncoding(tx, 1));
         journal.appendCommitRecord(tx, sync);
         sync = false;
      }
   }

   @Override
   public void oneBindingsJournal(Journal journal, List<PreparedTransactionInfo> preparedTransactions, List<RecordInfo> records) throws Exception {
      System.out.println("Generating max id at " + max * 2);
      journal.appendAddRecord(max, JournalRecordIds.ID_COUNTER_RECORD, BatchingIDGenerator.createIDEncodingSupport(max * 2), true);
   }
}
