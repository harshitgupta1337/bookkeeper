/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import io.netty.util.HashedWheelTimer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.net.NodeBase;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A placement policy use region information in the network topology for placing ensembles.
 *
 * @see EnsemblePlacementPolicy
 */
public class SameRackEnsemblePlacementPolicy extends RackawareEnsemblePlacementPolicy {
    static final Logger LOG = LoggerFactory.getLogger(SameRackEnsemblePlacementPolicy.class);

    public static final String REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE =
        "reppEnableDurabilityEnforcementInReplace";
    public static final String REPP_DISABLE_DURABILITY_FEATURE_NAME = "reppDisableDurabilityFeatureName";
    public static final String REPP_DISABLE_DURABILITY_ENFORCEMENT_FEATURE = "reppDisableDurabilityEnforcementFeature";
    public static final String REPP_ENABLE_VALIDATION = "reppEnableValidation";
    static final int REMOTE_NODE_IN_REORDER_SEQUENCE = 2;

    protected FeatureProvider featureProvider;
    protected boolean enableValidation = true;
    protected boolean enforceDurabilityInReplace = false;
    protected Feature disableDurabilityFeature;

    SameRackEnsemblePlacementPolicy() {
        super();
    }

    @Override
    public void handleBookiesThatLeft(Set<BookieSocketAddress> leftBookies) {
        super.handleBookiesThatLeft(leftBookies);
    }

    @Override
    public void handleBookiesThatJoined(Set<BookieSocketAddress> joinedBookies) {
        // node joined
        for (BookieSocketAddress addr : joinedBookies) {
            BookieNode node = createBookieNode(addr);
            topology.add(node);
            knownBookies.put(addr, node);
        }
    }

    @Override
    public SameRackEnsemblePlacementPolicy initialize(ClientConfiguration conf,
                                                         Optional<DNSToSwitchMapping> optionalDnsResolver,
                                                         HashedWheelTimer timer,
                                                         FeatureProvider featureProvider,
                                                         StatsLogger statsLogger) {
        LOG.info("Initializing SameRackEnsemblePlacementPolicy");
        super.initialize(conf, optionalDnsResolver, timer, featureProvider, statsLogger)
                .withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);
        enableValidation = conf.getBoolean(REPP_ENABLE_VALIDATION, true);
        this.featureProvider = featureProvider;
        this.disableDurabilityFeature = conf.getFeature(REPP_DISABLE_DURABILITY_ENFORCEMENT_FEATURE, null);
        if (null == disableDurabilityFeature) {
            this.disableDurabilityFeature =
                    featureProvider.getFeature(
                        conf.getString(REPP_DISABLE_DURABILITY_FEATURE_NAME,
                                BookKeeperConstants.FEATURE_REPP_DISABLE_DURABILITY_ENFORCEMENT));
        }
        return this;
    }

    @Override
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, Set<BookieSocketAddress> excludeBookies)
            throws BKException.BKNotEnoughBookiesException {
            if (ensembleSize != 1 || writeQuorumSize != 1 || ackQuorumSize != 1) {
                throw new IllegalArgumentException("All of following vals should be 1. ensembleSize = " + ensembleSize
                        + "writeQuorumSize = "+writeQuorumSize+" ackQuorumSize = "+ackQuorumSize);
            }
        ArrayList<BookieSocketAddress> ensemble = new ArrayList<BookieSocketAddress>();
        rwLock.readLock().lock();
        try {
            for (BookieSocketAddress addr : knownBookies.keySet()) {
                String networkLocation = knownBookies.get(addr).getNetworkLocation();
                String localNwLoc = localNode.getNetworkLocation();
                LOG.info("Candidate nwLoc = "+networkLocation+"; localNwLoc = " +localNwLoc);
                if (networkLocation.equals(localNwLoc))
                    ensemble.add(addr);
            }
            LOG.info(ensemble.size() + " bookies allocated successfully :");
            for (BookieSocketAddress b : ensemble) {
                LOG.info(b.getSocketAddress().toString());
            }
            return ensemble;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public BookieSocketAddress replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, Set<BookieSocketAddress> currentEnsemble,
            BookieSocketAddress bookieToReplace, Set<BookieSocketAddress> excludeBookies)
            throws BKException.BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            LOG.error("replaceBookie not implemented");
            return null;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public final DistributionSchedule.WriteSet reorderReadSequence(
            ArrayList<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        return super.reorderReadSequence(ensemble, bookiesHealthInfo, writeSet);
    }

    @Override
    public final DistributionSchedule.WriteSet reorderReadLACSequence(
            ArrayList<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        return super.reorderReadLACSequence(ensemble, bookiesHealthInfo, writeSet);
    }
}
