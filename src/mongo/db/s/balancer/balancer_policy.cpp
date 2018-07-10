/**
*    Copyright (C) 2018 MongoDB Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/db/s/balancer/balancer_policy.h"

#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/catalog/type_tags.h"
#include "mongo/util/log.h"
#include "mongo/util/stringutils.h"

namespace mongo {

using std::map;
using std::numeric_limits;
using std::set;
using std::string;
using std::vector;

namespace {
const size_t _maxMigrationsPerShard = 1;
bool _isShardBelowMaxMigrations(const std::map<ShardId, size_t>& numMigrationsMap,
                                const ShardId& shardId) {
    if (numMigrationsMap.count(shardId)) {
        size_t numMigrations = numMigrationsMap.find(shardId)->second;
        if (numMigrations >= _maxMigrationsPerShard)
            return false;
    }
    return true;
}
}  // namespace

DistributionStatus::DistributionStatus(NamespaceString nss, ShardToChunksMap shardToChunksMap)
    : _nss(std::move(nss)),
      _shardChunks(std::move(shardToChunksMap)),
      _zoneRanges(SimpleBSONObjComparator::kInstance.makeBSONObjIndexedMap<ZoneRange>()) {}

size_t DistributionStatus::totalChunksNotMaxedOut(const ShardStatisticsVector& shardStats) const {
    size_t total = 0;
    std::string tag;

    for (const auto& stat : shardStats) {
        Status status = BalancerPolicy::isShardSuitableReceiver(stat, tag);
        if (!status.isOK())
            continue;

        total += _shardChunks.at(stat.shardId).size();
    }

    return total;
}

size_t DistributionStatus::totalChunksWithTagNotMaxedOut(const ShardStatisticsVector& shardStats,
                                                         const std::string& tag) const {
    size_t total = 0;

    for (const auto& stat : shardStats) {
        auto status = BalancerPolicy::isShardSuitableReceiver(stat, tag);
        if (!status.isOK())
            continue;

        total += numberOfChunksInShardWithTag(stat.shardId, tag);
    }

    return total;
}

size_t DistributionStatus::numberOfChunksInShard(const ShardId& shardId) const {
    const auto& shardChunks = getChunks(shardId);
    return shardChunks.size();
}

size_t DistributionStatus::numberOfChunksInShardWithTag(const ShardId& shardId,
                                                        const std::string& tag) const {
    const auto& shardChunks = getChunks(shardId);

    size_t total = 0;

    for (const auto& chunk : shardChunks) {
        if (tag == getTagForChunk(chunk)) {
            total++;
        }
    }

    return total;
}
vector<ChunkType>& DistributionStatus::getChunks(const ShardId& shardId) {
    ShardToChunksMap::iterator i = _shardChunks.find(shardId);
    invariant(i != _shardChunks.end());

    return i->second;
}
const vector<ChunkType>& DistributionStatus::getChunks(const ShardId& shardId) const {
    ShardToChunksMap::const_iterator i = _shardChunks.find(shardId);
    invariant(i != _shardChunks.end());

    return i->second;
}
vector<ChunkType>::iterator DistributionStatus::moveChunkInDistributionAndUpdateMigrationsPerShard(
    const ShardId& donorShard,
    const ShardId& receiverShard,
    const std::vector<ChunkType>::iterator chunkIter,
    MigrateCandidatesSelection* migrateCandidatesSelection) {

    // Update migrationsPerShard.
    auto numMigrationsIter = migrateCandidatesSelection->migrationsPerShard.find(donorShard);
    if (numMigrationsIter == migrateCandidatesSelection->migrationsPerShard.end()) {
        migrateCandidatesSelection->migrationsPerShard.insert(std::make_pair(donorShard, 1));
    } else {
        numMigrationsIter->second = (numMigrationsIter->second + 1);
    }
    numMigrationsIter = migrateCandidatesSelection->migrationsPerShard.find(receiverShard);
    if (numMigrationsIter == migrateCandidatesSelection->migrationsPerShard.end()) {
        migrateCandidatesSelection->migrationsPerShard.insert(std::make_pair(receiverShard, 1));
    } else {
        numMigrationsIter->second = (numMigrationsIter->second + 1);
    }
    // Move chunk in distribution.
    _shardChunks.find(receiverShard)->second.push_back(*chunkIter);
    return _shardChunks.find(donorShard)->second.erase(chunkIter);
}
Status DistributionStatus::addRangeToZone(const ZoneRange& range) {
    const auto minIntersect = _zoneRanges.upper_bound(range.min);
    const auto maxIntersect = _zoneRanges.upper_bound(range.max);

    // Check for partial overlap
    if (minIntersect != maxIntersect) {
        invariant(minIntersect != _zoneRanges.end());
        const auto& intersectingRange =
            (SimpleBSONObjComparator::kInstance.evaluate(minIntersect->second.min < range.max))
            ? minIntersect->second
            : maxIntersect->second;

        if (SimpleBSONObjComparator::kInstance.evaluate(intersectingRange.min == range.min) &&
            SimpleBSONObjComparator::kInstance.evaluate(intersectingRange.max == range.max) &&
            intersectingRange.zone == range.zone) {
            return Status::OK();
        }

        return {ErrorCodes::RangeOverlapConflict,
                str::stream() << "Zone range: " << range.toString()
                              << " is overlapping with existing: "
                              << intersectingRange.toString()};
    }

    // Check for containment
    if (minIntersect != _zoneRanges.end()) {
        const ZoneRange& nextRange = minIntersect->second;
        if (SimpleBSONObjComparator::kInstance.evaluate(range.max > nextRange.min)) {
            invariant(SimpleBSONObjComparator::kInstance.evaluate(range.max < nextRange.max));
            return {ErrorCodes::RangeOverlapConflict,
                    str::stream() << "Zone range: " << range.toString()
                                  << " is overlapping with existing: "
                                  << nextRange.toString()};
        }
    }

    // This must be a new entry
    _zoneRanges.emplace(range.max.getOwned(), range);
    _allTags.insert(range.zone);
    return Status::OK();
}

string DistributionStatus::getTagForChunk(const ChunkType& chunk) const {
    const auto minIntersect = _zoneRanges.upper_bound(chunk.getMin());
    const auto maxIntersect = _zoneRanges.lower_bound(chunk.getMax());

    // We should never have a partial overlap with a chunk range. If it happens, treat it as if this
    // chunk doesn't belong to a tag
    if (minIntersect != maxIntersect) {
        return "";
    }

    if (minIntersect == _zoneRanges.end()) {
        return "";
    }

    const ZoneRange& intersectRange = minIntersect->second;

    // Check for containment
    if (SimpleBSONObjComparator::kInstance.evaluate(intersectRange.min <= chunk.getMin()) &&
        SimpleBSONObjComparator::kInstance.evaluate(chunk.getMax() <= intersectRange.max)) {
        return intersectRange.zone;
    }

    return "";
}

void DistributionStatus::report(BSONObjBuilder* builder) const {
    builder->append("ns", _nss.ns());

    // Report all shards
    BSONArrayBuilder shardArr(builder->subarrayStart("shards"));
    for (const auto& shardChunk : _shardChunks) {
        BSONObjBuilder shardEntry(shardArr.subobjStart());
        shardEntry.append("name", shardChunk.first.toString());

        BSONArrayBuilder chunkArr(shardEntry.subarrayStart("chunks"));
        for (const auto& chunk : shardChunk.second) {
            chunkArr.append(chunk.toConfigBSON());
        }
        chunkArr.doneFast();

        shardEntry.doneFast();
    }
    shardArr.doneFast();

    // Report all tags
    BSONArrayBuilder tagsArr(builder->subarrayStart("tags"));
    tagsArr.append(_allTags);
    tagsArr.doneFast();

    // Report all tag ranges
    BSONArrayBuilder tagRangesArr(builder->subarrayStart("tagRanges"));
    for (const auto& tagRange : _zoneRanges) {
        BSONObjBuilder tagRangeEntry(tagRangesArr.subobjStart());
        tagRangeEntry.append("tag", tagRange.second.zone);
        tagRangeEntry.append("mapKey", tagRange.first);
        tagRangeEntry.append("min", tagRange.second.min);
        tagRangeEntry.append("max", tagRange.second.max);
        tagRangeEntry.doneFast();
    }
    tagRangesArr.doneFast();
}

string DistributionStatus::toString() const {
    BSONObjBuilder builder;
    report(&builder);

    return builder.obj().toString();
}

Status BalancerPolicy::isShardSuitableReceiver(const ClusterStatistics::ShardStatistics& stat,
                                               const std::string& chunkTag) {
    if (stat.isSizeMaxed()) {
        return {ErrorCodes::IllegalOperation,
                str::stream() << stat.shardId << " has reached its maximum storage size."};
    }

    if (stat.isDraining) {
        return {ErrorCodes::IllegalOperation,
                str::stream() << stat.shardId << " is currently draining."};
    }

    if (!chunkTag.empty() && !stat.shardTags.count(chunkTag)) {
        return {ErrorCodes::IllegalOperation,
                str::stream() << stat.shardId << " is not in the correct zone " << chunkTag};
    }

    return Status::OK();
}

std::pair<ShardId, size_t> BalancerPolicy::_getLeastLoadedReceiverShard(
    const ShardStatisticsVector& shardStats,
    const DistributionStatus& distribution,
    const std::string& tag,
    MigrateCandidatesSelection* migrateCandidatesSelection) {
    ShardId best;
    unsigned minChunks = numeric_limits<unsigned>::max();

    for (const auto& stat : shardStats) {
        if (migrateCandidatesSelection->usedDonorShards.count(stat.shardId) ||
            !_isShardBelowMaxMigrations(migrateCandidatesSelection->migrationsPerShard,
                                        stat.shardId))
            continue;

        Status status = isShardSuitableReceiver(stat, tag);
        if (!status.isOK())
            continue;

        const size_t shardChunkCount = distribution.numberOfChunksInShardWithTag(stat.shardId, tag);

        if (shardChunkCount >= minChunks)
            continue;


        best = stat.shardId;
        minChunks = shardChunkCount;
    }

    return std::make_pair(best, minChunks);
}

std::pair<ShardId, size_t> BalancerPolicy::_getMostOverloadedDonorShard(
    const ShardStatisticsVector& shardStats,
    const DistributionStatus& distribution,
    const std::string& tag,
    MigrateCandidatesSelection* migrateCandidatesSelection,
    const std::set<ShardId>& unusableDonorShards) {
    ShardId worst;
    size_t maxChunks = 0;

    for (const auto& stat : shardStats) {
        if (migrateCandidatesSelection->usedRecipientShards.count(stat.shardId) ||
            !_isShardBelowMaxMigrations(migrateCandidatesSelection->migrationsPerShard,
                                        stat.shardId) ||
            unusableDonorShards.count(stat.shardId))
            continue;

        const size_t shardChunkCount = distribution.numberOfChunksInShardWithTag(stat.shardId, tag);
        if (shardChunkCount <= maxChunks)
            continue;

        worst = stat.shardId;
        maxChunks = shardChunkCount;
    }

    return std::make_pair(worst, maxChunks);
}

vector<MigrateInfo> BalancerPolicy::balance(
    const ShardStatisticsVector& shardStats,
    DistributionStatus& distribution,
    MigrateCandidatesSelection* migrateCandidatesSelection) {
    vector<MigrateInfo> migrations;

    // 1) Check for shards, which are in draining mode
    {
        for (const auto& stat : shardStats) {
            if (!stat.isDraining)
                continue;

            vector<ChunkType>& chunks = distribution.getChunks(stat.shardId);

            if (chunks.empty())
                continue;

            // Now we know we need to move the chunks off this shard, but only if permitted by the
            // tags policy
            auto chunkIter = chunks.begin();
            size_t numMigrations = 0;
            size_t numJumboChunks = 0;

            // Since we have to move all chunks, lets just do in order
            while (chunkIter != chunks.end()) {
                const auto& chunk = *chunkIter;
                if (chunk.getJumbo()) {
                    numJumboChunks++;
                    ++chunkIter;
                    continue;
                }
                const string tag = distribution.getTagForChunk(chunk);
                const auto ShardNumChunksPair = _getLeastLoadedReceiverShard(
                    shardStats, distribution, tag, migrateCandidatesSelection);
                const ShardId to = ShardNumChunksPair.first;
                if (!to.isValid()) {
                    if (migrations.empty()) {
                        warning() << "Chunk " << redact(chunk.toString())
                                  << " is on a draining shard, but no appropriate recipient found";
                    }
                    ++chunkIter;
                    continue;
                }

                invariant(to != stat.shardId);
                migrations.emplace_back(to, chunk);
                migrateCandidatesSelection->usedDonorShards.insert(stat.shardId);
                migrateCandidatesSelection->usedRecipientShards.insert(to);

                chunkIter = distribution.moveChunkInDistributionAndUpdateMigrationsPerShard(
                    stat.shardId, to, chunkIter, migrateCandidatesSelection);
                ++numMigrations;

                // No need to move other chunks on this shard
                if (_maxMigrationsPerShard == numMigrations)
                    break;
            }

            if (migrations.empty()) {
                warning() << "Unable to find any chunk to move from draining shard " << stat.shardId
                          << ". numJumboChunks: " << numJumboChunks;
            }
        }
    }

    // 2) Check for chunks, which are on the wrong shard and must be moved off of it
    if (!distribution.tags().empty()) {
        for (const auto& stat : shardStats) {
            // If shard already has received something then move the chunks off it on the next round
            // and also check if shard has exceeded numMigrationsPerShard.
            if (migrateCandidatesSelection->usedRecipientShards.count(stat.shardId) ||
                !_isShardBelowMaxMigrations(migrateCandidatesSelection->migrationsPerShard,
                                            stat.shardId))
                continue;

            vector<ChunkType>& chunks = distribution.getChunks(stat.shardId);
            auto it = chunks.begin();
            size_t numMigrations = 0;
            while (it != chunks.end()) {
                const auto& chunk = *it;
                const string tag = distribution.getTagForChunk(chunk);

                if (tag.empty() || stat.shardTags.count(tag)) {
                    ++it;
                    continue;
                }

                if (chunk.getJumbo()) {
                    warning() << "Chunk " << redact(chunk.toString()) << " violates zone "
                              << redact(tag) << ", but it is jumbo and cannot be moved";
                    ++it;
                    continue;
                }
                // Chunks here are not on the right shard
                const auto ShardNumChunksPair = _getLeastLoadedReceiverShard(
                    shardStats, distribution, tag, migrateCandidatesSelection);
                const ShardId to = ShardNumChunksPair.first;
                if (!to.isValid()) {
                    if (migrations.empty()) {
                        warning() << "Chunk " << redact(chunk.toString()) << " violates zone "
                                  << redact(tag) << ", but no appropriate recipient found";
                    }
                    ++it;
                    continue;
                }

                invariant(to != stat.shardId);
                migrations.emplace_back(to, chunk);
                migrateCandidatesSelection->usedDonorShards.insert(stat.shardId);
                migrateCandidatesSelection->usedRecipientShards.insert(to);

                it = distribution.moveChunkInDistributionAndUpdateMigrationsPerShard(
                    stat.shardId, to, it, migrateCandidatesSelection);
                ++numMigrations;
                // No need to move other chunks
                if (_maxMigrationsPerShard == numMigrations)
                    break;
            }
        }
    }

    // 3) For each tag balance. Get the total # of chunks and shards with that tag, so that for
    // each tag, the chunks are balanced on all the valid shards for that tag.

    vector<string> tagsPlusEmpty(distribution.tags().begin(), distribution.tags().end());
    tagsPlusEmpty.push_back("");

    for (const auto& tag : tagsPlusEmpty) {
        const size_t totalNumberOfChunksWithTag =
            (tag.empty() ? distribution.totalChunksNotMaxedOut(shardStats)
                         : distribution.totalChunksWithTagNotMaxedOut(shardStats, tag));

        size_t totalNumberOfShardsWithTag = 0;

        for (const auto& stat : shardStats) {
            if (tag.empty() || stat.shardTags.count(tag)) {
                Status status = isShardSuitableReceiver(stat, tag);
                if (status.isOK())
                    totalNumberOfShardsWithTag++;
            }
        }

        // Skip zones which have no shards assigned to them. This situation is not harmful, but
        // should not be possible so warn the operator to correct it.
        if (totalNumberOfShardsWithTag == 0) {
            if (!tag.empty()) {
                warning() << "Zone " << redact(tag) << " in collection " << distribution.nss()
                          << " has no assigned shards and chunks which fall into it cannot be "
                             "balanced. This should be corrected by either assigning shards to the "
                             "zone or by deleting it.";
            }
            continue;
        }

        // Calculate the rounded optimal number of chunks per shard
        const size_t idealNumberOfChunksPerShardForTag =
            (size_t)std::roundf(totalNumberOfChunksWithTag / (float)totalNumberOfShardsWithTag);
        set<ShardId> unusableDonorShards;
        while (_singleZoneBalance(shardStats,
                                  distribution,
                                  tag,
                                  idealNumberOfChunksPerShardForTag,
                                  &migrations,
                                  migrateCandidatesSelection,
                                  &unusableDonorShards))
            ;
    }

    return migrations;
}

boost::optional<MigrateInfo> BalancerPolicy::balanceSingleChunk(
    const ChunkType& chunk,
    const ShardStatisticsVector& shardStats,
    const DistributionStatus& distribution) {
    const string tag = distribution.getTagForChunk(chunk);
    MigrateCandidatesSelection migrateCandidatesSelection;
    const auto ShardNumChunksPair =
        _getLeastLoadedReceiverShard(shardStats, distribution, tag, &migrateCandidatesSelection);
    ShardId newShardId = ShardNumChunksPair.first;
    if (!newShardId.isValid() || newShardId == chunk.getShard()) {
        return boost::optional<MigrateInfo>();
    }

    return MigrateInfo(newShardId, chunk);
}

bool BalancerPolicy::_singleZoneBalance(const ShardStatisticsVector& shardStats,
                                        DistributionStatus& distribution,
                                        const std::string& tag,
                                        size_t idealNumberOfChunksPerShardForTag,
                                        std::vector<MigrateInfo>* migrations,
                                        MigrateCandidatesSelection* migrateCandidatesSelection,
                                        std::set<ShardId>* unusableDonorShards) {
    size_t numMigrations = 0;
    const auto shardNumChunksPair = _getMostOverloadedDonorShard(
        shardStats, distribution, tag, migrateCandidatesSelection, *unusableDonorShards);
    const auto from = shardNumChunksPair.first;
    if (!from.isValid())
        return false;

    size_t max = shardNumChunksPair.second;

    // Do not use a shard if it already has less entries than the optimal per-shard chunk count
    if (max <= idealNumberOfChunksPerShardForTag)
        return false;

    const auto ShardNumChunksPair =
        _getLeastLoadedReceiverShard(shardStats, distribution, tag, migrateCandidatesSelection);
    const ShardId to = ShardNumChunksPair.first;
    if (!to.isValid()) {
        if (migrations->empty()) {
            log() << "No available shards to take chunks for zone [" << tag << "]";
        }
        return false;
    }

    size_t min = ShardNumChunksPair.second;

    // Do not use a shard if it already has more entries than the optimal per-shard chunk count
    if (min >= idealNumberOfChunksPerShardForTag)
        return false;

    LOG(1) << "collection : " << distribution.nss().ns();
    LOG(1) << "zone       : " << tag;
    LOG(1) << "donor      : " << from << " chunks on " << max;
    LOG(1) << "receiver   : " << to << " chunks on " << min;
    LOG(1) << "ideal      : " << idealNumberOfChunksPerShardForTag;

    vector<ChunkType>& chunks = distribution.getChunks(from);

    size_t numJumboChunks = 0;
    auto it = chunks.begin();
    while (it != chunks.end()) {
        if ((min >= idealNumberOfChunksPerShardForTag) ||
            (max <= idealNumberOfChunksPerShardForTag))
            break;

        const auto& chunk = *it;
        if (distribution.getTagForChunk(chunk) != tag) {
            ++it;
            continue;
        }

        if (chunk.getJumbo()) {
            ++it;
            numJumboChunks++;
            continue;
        }
        invariant(to != from);
        migrations->emplace_back(to, chunk);
        migrateCandidatesSelection->usedDonorShards.insert(from);
        migrateCandidatesSelection->usedRecipientShards.insert(to);
        ++numMigrations;

        it = distribution.moveChunkInDistributionAndUpdateMigrationsPerShard(
            from, to, it, migrateCandidatesSelection);
        if (_maxMigrationsPerShard == numMigrations)
            break;

        --max;
        ++min;
    }

    // Should not consider donor shard anymore if only jumbo chunks left in this zone.
    if (numJumboChunks && numJumboChunks == distribution.numberOfChunksInShardWithTag(from, tag)) {
        unusableDonorShards->insert(from);
        warning() << "Shard: " << from << ", collection: " << distribution.nss().ns()
                  << " has only jumbo chunks for zone \'" << tag
                  << "\' and cannot be balanced. Jumbo chunks count: " << numJumboChunks;
    }

    return true;
}

ZoneRange::ZoneRange(const BSONObj& a_min, const BSONObj& a_max, const std::string& _zone)
    : min(a_min.getOwned()), max(a_max.getOwned()), zone(_zone) {}

string ZoneRange::toString() const {
    return str::stream() << min << " -->> " << max << "  on  " << zone;
}

MigrateInfo::MigrateInfo(const ShardId& a_to, const ChunkType& a_chunk) {
    invariant(a_chunk.validate());
    invariant(a_to.isValid());

    to = a_to;

    nss = a_chunk.getNS();
    from = a_chunk.getShard();
    minKey = a_chunk.getMin();
    maxKey = a_chunk.getMax();
    version = a_chunk.getVersion();
}

std::string MigrateInfo::getName() const {
    return ChunkType::genID(nss, minKey);
}

string MigrateInfo::toString() const {
    return str::stream() << nss.ns() << ": [" << minKey << ", " << maxKey << "), from " << from
                         << ", to " << to;
}

}  // namespace mongo
