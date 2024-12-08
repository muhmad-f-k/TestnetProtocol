// SPDX-License-Identifier: GPL-3.0
pragma solidity 0.8.8;

/**
 * @title WorkSystem Spot v1.5
 * @author 
 */

// File: dll/DLL.sol

library DLL {
    uint128 constant NULL_NODE_ID = 0;

    struct Node {
        uint128 next;
        uint128 prev;
    }

    struct SpottedData {
        mapping(uint128 => Node) dll;
    }

    function isEmpty(SpottedData storage self) public view returns(bool) {
        return getStart(self) == NULL_NODE_ID;
    }

    function contains(SpottedData storage self, uint128 _curr) public view returns(bool) {
        if (isEmpty(self) || _curr == NULL_NODE_ID) {
            return false;
        }

        bool isSingleNode = (getStart(self) == _curr) && (getEnd(self) == _curr);
        bool isNullNode = (getNext(self, _curr) == NULL_NODE_ID) && (getPrev(self, _curr) == NULL_NODE_ID);
        return isSingleNode || !isNullNode;
    }

    function getNext(SpottedData storage self, uint128 _curr) public view returns(uint128) {
        return self.dll[_curr].next;
    }

    function getPrev(SpottedData storage self, uint128 _curr) public view returns(uint128) {
        return self.dll[_curr].prev;
    }

    function getStart(SpottedData storage self) public view returns(uint128) {
        return getNext(self, NULL_NODE_ID);
    }

    function getEnd(SpottedData storage self) public view returns(uint256) {
        return getPrev(self, NULL_NODE_ID);
    }

    /**
    * @dev Inserts a new node between _prev and _next. When inserting a node already existing in 
      the list it will be automatically removed from the old position.
    * @param _prev the node which _new will be inserted after
    * @param _curr the id of the new node being inserted
    * @param _next the node which _new will be inserted before
    */
    function insert(
        SpottedData storage self,
        uint128 _prev,
        uint128 _curr,
        uint128 _next
    ) public {
        require(_curr != NULL_NODE_ID, "error: could not insert, 1");

        remove(self, _curr);

        require(_prev == NULL_NODE_ID || contains(self, _prev), "error: could not insert, 2");
        require(_next == NULL_NODE_ID || contains(self, _next), "error: could not insert, 3");

        require(getNext(self, _prev) == _next, "error: could not insert, 4");
        require(getPrev(self, _next) == _prev, "error: could not insert, 5");

        self.dll[_curr].prev = _prev;
        self.dll[_curr].next = _next;

        self.dll[_prev].next = _curr;
        self.dll[_next].prev = _curr;
    }

    function remove(SpottedData storage self, uint128 _curr) public {
        if (!contains(self, _curr)) {
            return;
        }

        uint128 next = getNext(self, _curr);
        uint128 prev = getPrev(self, _curr);

        self.dll[next].prev = prev;
        self.dll[prev].next = next;

        delete self.dll[_curr];
    }
}

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/utils/math/Math.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "./interfaces/IReputation.sol";
import "./interfaces/IRepManager.sol";
import "./interfaces/IDataSpotting.sol";
import "./interfaces/IDataQuality.sol";
import "./interfaces/IRewardManager.sol";
import "./interfaces/IStakeManager.sol";
import "./interfaces/IAddressManager.sol";
import "./interfaces/IParametersManager.sol";
import "./RandomAllocator.sol";

contract DataSpotting is Ownable, RandomAllocator, Pausable, IDataSpotting {
    // ============ EVENTS ============
    event _SpotSubmitted(uint256 indexed DataID, string file_hash, string URL_domain, address indexed sender);
    event _StakeAllocated(uint256 numTokens, address indexed voter);
    event _VotingRightsWithdrawn(uint256 numTokens, address indexed voter);
    event _TokensRescued(uint256 indexed DataID, address indexed voter);
    event _DataBatchDeleted(uint256 indexed batchID);
    event ParametersUpdated(address parameters);
    event BytesFailure(bytes bytesFailure);

    // ============ LIBRARIES ============
    using DLL for DLL.SpottedData;

    // ============ DATA STRUCTURES ============

    enum DataStatus {
        TBD,
        APPROVED,
        REJECTED,
        FLAGGED
    }

    struct TimeframeCounter {
        uint128 timestamp;
        uint128 counter;
    }

    struct WorkerState {
        uint128 allocated_work_batch;
        uint64 last_interaction_date;
        uint16 succeeding_novote_count;
        bool registered;
        bool unregistration_request;
        bool isWorkerSeen;
        uint64 registration_date;
        uint64 allocated_batch_counter;
        uint64 majority_counter;
        uint64 minority_counter;
    }

    struct BatchMetadata {
        uint128 start_idx;
        uint32 counter;
        uint32 item_count;
        uint16 uncommited_workers;
        uint16 unrevealed_workers;
        bool complete;
        bool checked;
        bool allocated_to_work;
        DataStatus status; // state of the vote
        uint16 votesFor; // tally of spot-check-votes supporting proposal
        uint16 votesAgainst; // tally of spot-check-votes countering proposal
        uint64 commitEndDate; // expiration date of commit period for poll
        uint64 revealEndDate; // expiration date of reveal period for poll
        string batchIPFSfile; // updated during SpotChecking
    }

    struct SpottedData {
        uint64 timestamp; 
        uint64 item_count;
        DataStatus status; 
        string extra; 
        address author; 
        string ipfs_hash; 
        string URL_domain; 
    }

    struct VoteSubmission {
        bool commited;
        bool revealed;
        uint8 vote;
        uint32 batchCount;
        string newFile;
        string batchFrom;
    }

    struct WorkerStatus {
        bool isAvailableWorker;
        bool isBusyWorker;
        bool isToUnregisterWorker;
        bool isActiveWorker;
        uint32 availableWorkersIndex;
        uint32 busyWorkersIndex;
        uint32 toUnregisterWorkersIndex;
        uint32 activeWorkersIndex;
    }

    // ====================================
    //        GLOBAL STATE VARIABLES
    // ====================================

    uint64 public LastAllocationTime = 0;
    uint16 constant NB_TIMEFRAMES = 15;
    uint16 constant MAX_MASTER_DEPTH = 3;
    TimeframeCounter[NB_TIMEFRAMES] public GlobalSpotFlowManager; 
    TimeframeCounter[NB_TIMEFRAMES] public ItemFlowManager;

    address[] public AllWorkersList;
    address[] public activeWorkers;
    uint256 public activeWorkersCount = 0;
    uint256 activeWorkerWindow = 2 hours;

    mapping(bytes32 => uint256) store;
    mapping(uint128 => SpottedData) public SpotsMapping;
    mapping(uint128 => BatchMetadata) public DataBatch; 

    mapping(address => DLL.SpottedData) private dllMap;
    mapping(address => WorkerState) public WorkersState;
    mapping(address => TimeframeCounter[NB_TIMEFRAMES]) public WorkersSpotFlowManager;
    mapping(address => uint256) public SystemStakedTokenBalance; 

    mapping(address => WorkerStatus) public WorkersStatus;

    uint128 public DataNonce = 0;
    uint128 public LastBatchCounter = 1;
    uint128 public BatchDeletionCursor = 1;

    uint256 public LastRandomSeed = 0;
    uint256 public AllTxsCounter = 0;
    uint256 public AllItemCounter = 0;

    uint256 constant BYTES_256 = 32;
    uint256 public BytesUsed = 404;

    uint128 public MAX_INDEX_RANGE_BATCHS = 10000;
    uint128 public MAX_INDEX_RANGE_SPOTS = 10000 * 30;

    bool public STAKING_REQUIREMENT_TOGGLE_ENABLED = false;
    bool public InstantSpotRewards = true;
    uint16 public InstantSpotRewardsDivider = 30;
    uint256 public NB_BATCH_TO_TRIGGER_GARBAGE_COLLECTION = 1000;
    uint256 private MIN_OFFSET_DELETION_CURSOR = 50;

    IERC20 public token;
    IParametersManager public Parameters;

    constructor(address EXDT_token_) {
        require(address(EXDT_token_) != address(0));
        token = IERC20(EXDT_token_);
    }

    function toggleSystemPause() public onlyOwner {
        if (paused()) {
            _unpause();
        } else {
            _pause();
        }
    }

    function updateParametersManager(address addr) public onlyOwner {
        require(addr != address(0), "addr must be non zero");
        Parameters = IParametersManager(addr);
        emit ParametersUpdated(addr);
    }

    function updateInstantSpotRewards(bool state_, uint16 divider_) public onlyOwner {
        InstantSpotRewards = state_;
        InstantSpotRewardsDivider = divider_;
    }

    function updateActiveWorkerWindow(uint256 new_window_) public onlyOwner {
        require(new_window_> 10 minutes , "new window must be > 1 minute");
        activeWorkerWindow = new_window_;
    }

    function updateGarbageCollectionThreshold(uint256 NewGarbageCollectTreshold_) public onlyOwner {
        require(NewGarbageCollectTreshold_ > 100, "NewGarbageCollectTreshold_ must be > 100");
        NB_BATCH_TO_TRIGGER_GARBAGE_COLLECTION = NewGarbageCollectTreshold_;
    }

    function getAttribute(bytes32 _UUID, string memory _attrName) public view returns(uint256) {
        bytes32 key = keccak256(abi.encodePacked(_UUID, _attrName));
        return store[key];
    }

    function setAttribute(
        bytes32 _UUID,
        string memory _attrName,
        uint256 _attrVal
    ) internal {
        bytes32 key = keccak256(abi.encodePacked(_UUID, _attrName));
        store[key] = _attrVal;
    }

    function resetAttribute(
        bytes32 _UUID,
        string memory _attrName
    ) internal {
        bytes32 key = keccak256(abi.encodePacked(_UUID, _attrName));
        delete store[key];
    }

    function _retrieveSFuel() internal {
        require(IParametersManager(address(0)) != Parameters, "Parameters Manager must be set.");
        address sFuelAddress;
        sFuelAddress = Parameters.getsFuelSystem();
        require(sFuelAddress != address(0), "sFuel: null Address Not Valid");
        (bool success1, ) = sFuelAddress.call(abi.encodeWithSignature("retrieveSFuel(address)", payable(msg.sender)));
        (bool success2, ) = sFuelAddress.call(
            abi.encodeWithSignature("retrieveSFuel(address payable)", payable(msg.sender))
        );
        require((success1 || success2), "receiver rejected _retrieveSFuel call");
    }

    function _ModB(uint128 BatchId) private view returns(uint128) {
        return BatchId % MAX_INDEX_RANGE_BATCHS;
    }

    function _ModS(uint128 SpotId) private view returns(uint128) {
        return SpotId % MAX_INDEX_RANGE_SPOTS;
    }

    function SelectAddressForUser(address _worker, uint256 _TokensAmountToAllocate) public view returns(address) {
        require(IParametersManager(address(0)) != Parameters, "Parameters Manager must be set.");
        require(Parameters.getAddressManager() != address(0), "AddressManager is null in Parameters");
        require(Parameters.getStakeManager() != address(0), "StakeManager is null in Parameters");
        IStakeManager _StakeManager = IStakeManager(Parameters.getStakeManager());
        IAddressManager _AddressManager = IAddressManager(Parameters.getAddressManager());

        address _SelectedAddress = _worker;
        address _CurrentAddress = _worker;

        for (uint256 i = 0; i < MAX_MASTER_DEPTH; i++) {
            uint256 _CurrentAvailableStake = _StakeManager.AvailableStakedAmountOf(_CurrentAddress);

            if (SystemStakedTokenBalance[_CurrentAddress] >= _TokensAmountToAllocate) {
                _SelectedAddress = _CurrentAddress;
                break;
            } else if (
                SystemStakedTokenBalance[_CurrentAddress] <= _TokensAmountToAllocate &&
                SystemStakedTokenBalance[_CurrentAddress] > 0
            ) {
                uint256 remainderAmountToAllocate = _TokensAmountToAllocate - SystemStakedTokenBalance[_CurrentAddress];
                if (_CurrentAvailableStake >= remainderAmountToAllocate) {
                    _SelectedAddress = _CurrentAddress;
                    break;
                }
            }

            if (_CurrentAvailableStake >= _TokensAmountToAllocate) {
                _SelectedAddress = _CurrentAddress;
                break;
            }

            _CurrentAddress = _AddressManager.getMaster(_CurrentAddress);

            if (_CurrentAddress == address(0)) {
                break; 
            }
        }

        return _SelectedAddress;
    }

    function IsAddressKicked(address user_) public view returns(bool status) {
        bool status_ = false;
        WorkerState memory worker_state = WorkersState[user_];
        if ((worker_state.succeeding_novote_count >= Parameters.get_MAX_SUCCEEDING_NOVOTES() &&
                ((block.timestamp - worker_state.registration_date) <
                    Parameters.get_NOVOTE_REGISTRATION_WAIT_DURATION()))) {
            status_ = true;
        }
        return status_;
    }

    function AmIKicked() public view returns(bool status) {
        return IsAddressKicked(msg.sender);
    }

    function handleStakingRequirement(address worker) internal {
        if (STAKING_REQUIREMENT_TOGGLE_ENABLED) {
            uint256 _numTokens = Parameters.get_SPOT_MIN_STAKE();
            address _selectedAddress = SelectAddressForUser(worker, _numTokens);

            if (SystemStakedTokenBalance[_selectedAddress] < _numTokens) {
                uint256 remainder = _numTokens - SystemStakedTokenBalance[_selectedAddress];
                requestAllocatedStake(remainder, _selectedAddress);
            }
        }
    }

    function isInActiveWorkers(address _worker) public view returns(bool) {
        return WorkersStatus[_worker].isActiveWorker;
    }

    function PushInActiveWorkers(address _worker) internal {
        require(_worker != address(0), "Error: Can't push the null address in active workers");
        if (!isInActiveWorkers(_worker)) {
            activeWorkers.push(_worker);
            WorkersStatus[_worker].isActiveWorker = true;
            WorkersStatus[_worker].activeWorkersIndex = uint32(activeWorkers.length - 1);
        }
    }

    uint32 private REMOVED_WORKER_INDEX_VALUE = 2 ** 32 - 1;

    function PopFromActiveWorkers(address _worker) internal {
        WorkerStatus storage workerStatus = WorkersStatus[_worker];
        if (workerStatus.isActiveWorker) {
            uint32 PreviousIndex = workerStatus.activeWorkersIndex;
            address SwappedWorkerAtIndex = activeWorkers[activeWorkers.length - 1];

            workerStatus.isActiveWorker = false;
            workerStatus.activeWorkersIndex = REMOVED_WORKER_INDEX_VALUE;

            if (activeWorkers.length >= 2) {
                activeWorkers[PreviousIndex] = SwappedWorkerAtIndex; 
                WorkersStatus[SwappedWorkerAtIndex].activeWorkersIndex = PreviousIndex;
            }

            activeWorkers.pop();  
        }
    }

    function updateActiveWorkerStatus(address userAddress) internal {
        WorkerState memory worker_state = WorkersState[userAddress];
        if(block.timestamp > worker_state.last_interaction_date + activeWorkerWindow){
            PopFromActiveWorkers(userAddress);
            if( activeWorkersCount >= 1 ){
                activeWorkersCount -= 1 ;
            }
        }
    }

    function updateActiveWorkersBatch(uint256 start_idx, uint256 nb_iterations) internal {
        require(start_idx < activeWorkers.length, "Start index is out of bounds.");
        uint256 endIndex = start_idx + nb_iterations;
        
        if(endIndex >= activeWorkers.length) {
            endIndex = activeWorkers.length -1;
        }
        
        for (uint256 i = start_idx; i < endIndex; i++) {
            address worker_ = activeWorkers[i];
            updateActiveWorkerStatus(worker_);
        }
    }

    function deleteOldData(uint128 iteration_count) internal {
        if ((BatchDeletionCursor < (LastBatchCounter - MIN_OFFSET_DELETION_CURSOR))) {
            for (uint128 i = 0; i < iteration_count; i++) {
                uint128 _deletion_index = BatchDeletionCursor;
                uint128 start_batch_idx = DataBatch[_ModB(_deletion_index)].start_idx;
                uint128 end_batch_idx = DataBatch[_ModB(_deletion_index)].start_idx +
                    DataBatch[_ModB(_deletion_index)].counter;
                for (uint128 l = start_batch_idx; l < end_batch_idx; l++) {
                    delete SpotsMapping[_ModS(l)];
                }
                delete DataBatch[_ModB(_deletion_index)];
                emit _DataBatchDeleted(_deletion_index);
                BatchDeletionCursor = BatchDeletionCursor + 1;
            }
        }
    }

    function deleteWorkersAtIndex(uint256 index_) public onlyOwner {
        address worker_at_index = AllWorkersList[index_];
        address SwappedWorkerAtIndex = AllWorkersList[AllWorkersList.length - 1];
        if (AllWorkersList.length >= 2) {
            AllWorkersList[index_] = SwappedWorkerAtIndex; 
        }

        AllWorkersList.pop(); 
        deleteWorkersStatus(worker_at_index);
    }

    function deleteManyWorkersAtIndex(uint256[] memory indices_) public onlyOwner {
        for (uint256 i = 0; i < indices_.length; i++) {
            uint256 _index = indices_[i];
            deleteWorkersAtIndex(_index);
        }
    }

    function deleteWorkersStatus(address user_) public onlyOwner {
        delete WorkersStatus[user_];
    }

    function deleteManyWorkersStatus(address[] memory users_) public onlyOwner {
        for (uint256 i = 0; i < users_.length; i++) {
            address _user = users_[i];
            deleteWorkersStatus(_user);
        }
    }

    function deleteWorkersState(address user_) public onlyOwner {
        delete WorkersState[user_];
        delete dllMap[user_];
        delete WorkersSpotFlowManager[user_];
        delete SystemStakedTokenBalance[user_];
    }

    function deleteManyWorkersState(address[] memory users_) public onlyOwner {
        for (uint256 i = 0; i < users_.length; i++) {
            address _user = users_[i];
            deleteWorkersState(_user);
        }
    }

    function TriggerUpdate(uint256 start_idx, uint256 nb_iter) public {
        require(IParametersManager(address(0)) != Parameters, "Parameters Manager must be set.");
        updateGlobalSpotFlow();
        updateActiveWorkersBatch(start_idx, nb_iter);
        _retrieveSFuel();
    }

    function AreStringsEqual(string memory _a, string memory _b) public pure returns(bool) {
        if (keccak256(abi.encodePacked(_a)) == keccak256(abi.encodePacked(_b))) {
            return true;
        } else {
            return false;
        }
    }

    function updateGlobalSpotFlow() public {
        require(IParametersManager(address(0)) != Parameters, "Parameters Manager must be set.");
        uint256 last_timeframe_idx_ = GlobalSpotFlowManager.length - 1;
        uint256 mostRecentTimestamp_ = GlobalSpotFlowManager[last_timeframe_idx_].timestamp;
        if ((uint64(block.timestamp) - mostRecentTimestamp_) > Parameters.get_SPOT_TIMEFRAME_DURATION()) {
            for (uint256 i = 0; i < (GlobalSpotFlowManager.length - 1); i++) {
                GlobalSpotFlowManager[i] = GlobalSpotFlowManager[i + 1];
            }
            GlobalSpotFlowManager[last_timeframe_idx_].timestamp = uint64(block.timestamp);
            GlobalSpotFlowManager[last_timeframe_idx_].counter = 0;
        }
    }

    function getGlobalPeriodSpotCount() public view returns(uint256) {
        uint256 total = 0;
        for (uint256 i = 0; i < GlobalSpotFlowManager.length; i++) {
            total += GlobalSpotFlowManager[i].counter;
        }
        return total;
    }

    function getPeriodItemCount() public view returns(uint256) {
        uint256 total = 0;
        for (uint256 i = 0; i < ItemFlowManager.length; i++) {
            total += ItemFlowManager[i].counter;
        }
        return total;
    }

    function addtoItemCounter(uint128 item_count_to_add)
    internal {
        AllTxsCounter += 1;
        AllItemCounter += item_count_to_add;
        ItemFlowManager[ItemFlowManager.length - 1].counter += item_count_to_add;
        updateItemCount();
    }

    function updateItemCount() public {
        require(IParametersManager(address(0)) != Parameters, "Parameters Manager must be set.");
        uint256 last_timeframe_idx_ = ItemFlowManager.length - 1;
        uint256 mostRecentTimestamp_ = ItemFlowManager[last_timeframe_idx_].timestamp;
        if ((uint64(block.timestamp) - mostRecentTimestamp_) > Parameters.get_SPOT_TIMEFRAME_DURATION()) {
            for (uint256 i = 0; i < (ItemFlowManager.length - 1); i++) {
                ItemFlowManager[i] = ItemFlowManager[i + 1];
            }
            ItemFlowManager[last_timeframe_idx_].timestamp = uint64(block.timestamp);
            ItemFlowManager[last_timeframe_idx_].counter = 0;
        }
    }

    function updateUserSpotFlow(address user_) public {
        require(IParametersManager(address(0)) != Parameters, "Parameters Manager must be set.");
        TimeframeCounter[NB_TIMEFRAMES] storage UserSpotFlowManager = WorkersSpotFlowManager[user_];

        uint256 last_timeframe_idx_ = UserSpotFlowManager.length - 1;
        uint256 mostRecentTimestamp_ = UserSpotFlowManager[last_timeframe_idx_].timestamp;
        if ((block.timestamp - mostRecentTimestamp_) > Parameters.get_SPOT_TIMEFRAME_DURATION()) {
            for (uint256 i = 0; i < (UserSpotFlowManager.length - 1); i++) {
                UserSpotFlowManager[i] = UserSpotFlowManager[i + 1];
            }
            UserSpotFlowManager[last_timeframe_idx_].timestamp = uint64(block.timestamp);
            UserSpotFlowManager[last_timeframe_idx_].counter = 0;
        }
    }

    function getUserPeriodSpotCount(address user_) public view returns(uint256) {
        TimeframeCounter[NB_TIMEFRAMES] storage UserSpotFlowManager = WorkersSpotFlowManager[user_];
        uint256 total = 0;
        for (uint256 i = 0; i < UserSpotFlowManager.length; i++) {
            total += UserSpotFlowManager[i].counter;
        }
        return total;
    }

    /**
     * @notice Modified SpotData function with Off-by-One Fix
     */
    function SpotData(
        string[] memory file_hashs_,
        string[] calldata URL_domains_,
        uint64[] memory item_counts_,
        string memory extra_
    ) public
    whenNotPaused()
    returns(uint256 Dataid_) {
        require(IParametersManager(address(0)) != Parameters, "Parameters Manager must be set.");
        require(Parameters.get_SPOT_TOGGLE_ENABLED(), "Spotting is not currently enabled by Owner");
        require(file_hashs_.length == URL_domains_.length, "input arrays must be of same length (1)");
        require(file_hashs_.length == item_counts_.length, "input arrays must be of same length (2)");

        updateGlobalSpotFlow();
        require(
            getGlobalPeriodSpotCount() < Parameters.get_SPOT_GLOBAL_MAX_SPOT_PER_PERIOD(),
            "Global limit: exceeded max data per hour, retry later."
        );

        uint256 _numTokens = Parameters.get_SPOT_MIN_STAKE();
        address _selectedAddress = SelectAddressForUser(msg.sender, _numTokens);
        updateUserSpotFlow(_selectedAddress);

        uint128 _batch_counter = LastBatchCounter;

        if (
            getUserPeriodSpotCount(_selectedAddress) < Parameters.get_SPOT_MAX_SPOT_PER_USER_PER_PERIOD() &&
            getGlobalPeriodSpotCount() < Parameters.get_SPOT_GLOBAL_MAX_SPOT_PER_PERIOD()
        ) {
            if (STAKING_REQUIREMENT_TOGGLE_ENABLED) {
                if (SystemStakedTokenBalance[_selectedAddress] < _numTokens) {
                    uint256 remainder = _numTokens - SystemStakedTokenBalance[_selectedAddress];
                    requestAllocatedStake(remainder, _selectedAddress);
                }
            }

            for (uint64 i = 0; i < file_hashs_.length; i++) {
                string memory file_hash = file_hashs_[i];
                string memory URL_domain_ = URL_domains_[i];
                uint64 item_count_ = item_counts_[i];

                SpotsMapping[_ModS(DataNonce)] = SpottedData({
                    ipfs_hash: file_hash,
                    author: msg.sender,
                    timestamp: uint64(block.timestamp),
                    item_count: item_count_,
                    URL_domain: URL_domain_,
                    extra: extra_,
                    status: DataStatus.TBD
                });

                BatchMetadata storage current_data_batch = DataBatch[_ModB(_batch_counter)];

                // FIX: Set start_idx as soon as the first spot of the batch is added
                // This ensures start_idx always points to the first spot in the batch
                if (current_data_batch.counter == 0) {
                    current_data_batch.start_idx = DataNonce;  // <-- FIX APPLIED HERE
                }

                if (current_data_batch.counter < Parameters.get_SPOT_DATA_BATCH_SIZE()) {
                    current_data_batch.counter += 1;
                }

                if (current_data_batch.counter >= Parameters.get_SPOT_DATA_BATCH_SIZE()) {
                    current_data_batch.complete = true;
                    current_data_batch.checked = false;
                    LastBatchCounter += 1;
                    delete DataBatch[_ModB(LastBatchCounter)];
                    // Note: We do NOT update start_idx here anymore.
                }

                DataNonce = DataNonce + 1;
                GlobalSpotFlowManager[GlobalSpotFlowManager.length - 1].counter += 1;
                TimeframeCounter[NB_TIMEFRAMES] storage UserSpotFlowManager = WorkersSpotFlowManager[_selectedAddress];
                UserSpotFlowManager[UserSpotFlowManager.length - 1].counter += 1;
                addtoItemCounter(item_count_);

                if (InstantSpotRewards && item_count_>0 && item_count_ <= 1000) {
                    address spot_author_ = msg.sender;
                    IAddressManager _AddressManager = IAddressManager(Parameters.getAddressManager());
                    IRepManager _RepManager = IRepManager(Parameters.getRepManager());
                    IRewardManager _RewardManager = IRewardManager(Parameters.getRewardManager());
                    address spot_author_master_ = _AddressManager.FetchHighestMaster(spot_author_);
                    uint256 rewardAmount = (Parameters.get_SPOT_MIN_REWARD_SpotData() * item_count_) /
                        InstantSpotRewardsDivider;
                    uint256 repAmount = (Parameters.get_SPOT_MIN_REP_SpotData() * item_count_) /
                        InstantSpotRewardsDivider;
                    require(
                        _RepManager.mintReputationForWork(repAmount, spot_author_master_, ""),
                        "could not reward REP in ValidateDataBatch, 2.a"
                    );
                    require(
                        _RewardManager.ProxyAddReward(rewardAmount, spot_author_master_),
                        "could not reward token in ValidateDataBatch, 2.b"
                    );
                }

                emit _SpotSubmitted(DataNonce, file_hash, URL_domain_, _selectedAddress);
            }
        }
        WorkerState storage worker_state = WorkersState[msg.sender];
        if (!worker_state.isWorkerSeen) {
            AllWorkersList.push(msg.sender);
            worker_state.isWorkerSeen = true;
        }
        if (!isInActiveWorkers(msg.sender)){
            PushInActiveWorkers(msg.sender);
            activeWorkersCount += 1;
        }
        worker_state.last_interaction_date = uint64(block.timestamp);

        _retrieveSFuel();
        AllTxsCounter += 1;
        return DataNonce;
    }

    function requestAllocatedStake(uint256 _numTokens, address user_) internal {
        require(Parameters.getStakeManager() != address(0), "StakeManager is null in Parameters");
        IStakeManager _StakeManager = IStakeManager(Parameters.getStakeManager());
        require(
            _StakeManager.ProxyStakeAllocate(_numTokens, user_),
            "Could not request enough allocated stake, requestAllocatedStake"
        );

        SystemStakedTokenBalance[user_] += _numTokens;
        emit _StakeAllocated(_numTokens, user_);
    }

    function withdrawVotingRights(uint256 _numTokens) public {
        require(IParametersManager(address(0)) != Parameters, "Parameters Manager must be set.");
        address _selectedAddress = SelectAddressForUser(msg.sender, _numTokens);
        require(_selectedAddress != address(0), "Error: _selectedAddress is null during withdrawVotingRights");
        uint256 availableTokens = SystemStakedTokenBalance[_selectedAddress] - getLockedTokens(_selectedAddress);
        require(availableTokens >= _numTokens, "availableTokens should be >= _numTokens");

        IStakeManager _StakeManager = IStakeManager(Parameters.getStakeManager());
        SystemStakedTokenBalance[_selectedAddress] -= _numTokens;
        require(
            _StakeManager.ProxyStakeDeallocate(_numTokens, _selectedAddress),
            "Could not withdrawVotingRights through ProxyStakeDeallocate"
        );
        _retrieveSFuel();
        emit _VotingRightsWithdrawn(_numTokens, _selectedAddress);
    }

    function getBytesUsed() public view returns(uint256 storage_size) {
        return BytesUsed;
    }

    function getSystemTokenBalance(address user_) public view returns(uint256 tokens) {
        return (uint256(SystemStakedTokenBalance[user_]));
    }

    function rescueTokens(uint128 _DataBatchId) public {
        require(
            DataBatch[_ModB(_DataBatchId)].status == DataStatus.APPROVED,
            "given DataBatch should be APPROVED"
        );
        require(dllMap[msg.sender].contains(_DataBatchId), "dllMap: does not contain _DataBatchId");

        dllMap[msg.sender].remove(_DataBatchId);

        emit _TokensRescued(_DataBatchId, msg.sender);
    }

    function rescueTokensInMultipleDatas(uint128[] memory _DataBatchIDs) public {
        for (uint256 i = 0; i < _DataBatchIDs.length; i++) {
            rescueTokens(_DataBatchIDs[i]);
        }
    }

    function validPosition(
        uint128 _prevID,
        uint128 _nextID,
        address _voter,
        uint256 _numTokens
    ) public view returns(bool APPROVED) {
        bool prevValid = (_numTokens >= getNumTokens(_voter, _prevID));
        bool nextValid = (_numTokens <= getNumTokens(_voter, _nextID) || _nextID == 0);
        return prevValid && nextValid;
    }

    function isPassed(uint128 _DataBatchId) public view returns(bool passed) {
        BatchMetadata memory batch_ = DataBatch[_ModB(_DataBatchId)];
        return (100 * batch_.votesFor) > (Parameters.getVoteQuorum() * (batch_.votesFor + batch_.votesAgainst));
    }

    function getLastDataId() public view returns(uint256 DataId) {
        return DataNonce;
    }

    function getLastBatchId() public view returns(uint256 LastBatchId) {
        return LastBatchCounter;
    }

    function getBatchByID(uint128 _DataBatchId) public view override returns(BatchMetadata memory batch) {
        return DataBatch[_ModB(_DataBatchId)];
    }

    function getBatchIPFSFileByID(uint128 _DataBatchId) public view override returns(string memory) {
        BatchMetadata memory batch_ = DataBatch[_ModB(_DataBatchId)];
        return batch_.batchIPFSfile;
    }

    function getDataByID(uint128 _DataId) public view override returns(SpottedData memory data) {
        return SpotsMapping[_ModS(_DataId)];
    }

    function getLastCheckedBatchId() public view returns(uint256 LastCheckedBatchId) {
        return LastBatchCounter;
    }

    function getTxCounter() public view returns(uint256 Counter) {
        return AllTxsCounter;
    }

    function getItemCounter() public view returns(uint256 Counter) {
        return AllItemCounter;
    }

    function DataExists(uint128 _DataBatchId) public view returns(bool exists) {
        return (DataBatch[_ModB(_DataBatchId)].complete);
    }

    function getNumTokens(address _voter, uint128 _DataBatchId) public view returns(uint256 numTokens) {
        return getAttribute(attrUUID(_voter, _DataBatchId), "numTokens");
    }

    function getLastNode(address _voter) public view returns(uint128 DataID) {
        return dllMap[_voter].getPrev(0);
    }

    function getLockedTokens(address _voter) public view returns(uint256 numTokens) {
        return getNumTokens(_voter, getLastNode(_voter));
    }

    function getInsertPointForNumTokens(
        address _voter,
        uint256 _numTokens,
        uint128 _DataBatchId
    ) public view returns(uint256 prevNode) {
        uint128 nodeID = getLastNode(_voter);
        uint256 tokensInNode = getNumTokens(_voter, nodeID);

        while (nodeID != 0) {
            tokensInNode = getNumTokens(_voter, nodeID);
            if (tokensInNode <= _numTokens) {
                if (nodeID == _DataBatchId) {
                    nodeID = dllMap[_voter].getPrev(nodeID);
                }
                return nodeID;
            }
            nodeID = dllMap[_voter].getPrev(nodeID);
        }

        return nodeID;
    }

    function isExpired(uint256 _terminationDate) public view returns(bool expired) {
        return (block.timestamp > _terminationDate);
    }

    function attrUUID(address user_, uint128 _DataBatchId) public pure returns(bytes32 UUID) {
        return keccak256(abi.encodePacked(user_, _DataBatchId));
    }
}
