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

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.message.impl.CoreMessagePersister;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;

@Command(name = "housekeep", description = "This will give you an opportunity of using an extension of org.apache.activemq.artemis.cli.commands.tools.HousekeepPlugin to cleanup the journal")
public class HousekeepData extends OptionalLocking {

   @Option(name = "--plugin", description = "The name of the plugin to perform the cleanup", required = true)
   private String plugin;

   static {
      MessagePersister.registerPersister(CoreMessagePersister.getInstance());
   }

   HousekeepPlugin housekeepPlugin;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      try {
         housekeepPlugin = (HousekeepPlugin) HousekeepData.class.getClassLoader().loadClass(plugin).newInstance();
         houseKeep(new File(getBinding()), new File(getJournal()));
      } catch (Exception e) {
         treatError(e, "data", "print");
      }
      return null;
   }

   public void houseKeep(File bindingsDirectory, File messagesDirectory) throws Exception {
      SequentialFileFactory messagesFF = new NIOSequentialFileFactory(messagesDirectory, null, 1);
      List<RecordInfo> records = new LinkedList<>();
      List<PreparedTransactionInfo> preparedTransactions = new LinkedList<>();

      Journal journal = loadJournal(messagesFF, records, preparedTransactions, "activemq-data", "amq");
      try {
         housekeepPlugin.onMessageJournal(journal, preparedTransactions, records);
      } catch (Exception e) {
         e.printStackTrace();
      }

      journal.stop();

      messagesFF = new NIOSequentialFileFactory(bindingsDirectory, null, 1);
      records.clear();
      preparedTransactions.clear();

      journal = loadJournal(messagesFF, records, preparedTransactions, "activemq-bindings", "bindings");
      try {
         housekeepPlugin.oneBindingsJournal(journal, preparedTransactions, records);
      } catch (Exception e) {
         e.printStackTrace();
      }

      journal.stop();

   }

   public Journal loadJournal(SequentialFileFactory messagesFF,
                              List<RecordInfo> records,
                              List<PreparedTransactionInfo> preparedTransactions,
                              String prefix,
                              String suffix) throws Exception {
      // Will use only default values. The load function should adapt to anything different
      ConfigurationImpl defaultValues = new ConfigurationImpl();
      defaultValues.setJournalMinFiles(2);
      defaultValues.setJournalPoolFiles(4);

      JournalImpl journal = new JournalImpl(defaultValues.getJournalFileSize(), defaultValues.getJournalMinFiles(), defaultValues.getJournalPoolFiles(), 0, 0, messagesFF, prefix, suffix, 1);

      journal.start();

      journal.load(records, preparedTransactions, null);
      return journal;
   }

}
