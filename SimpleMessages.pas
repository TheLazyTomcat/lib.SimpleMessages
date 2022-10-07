unit SimpleMessages;

{$IF Defined(WINDOWS) or Defined(MSWINDOWS)}
  {$DEFINE Windows}
{$ELSEIF Defined(LINUX) and Defined(FPC)}
  {$DEFINE Linux}
{$ELSE}
  {$MESSAGE FATAL 'Unsupported operating system.'}
{$IFEND}

{$IFDEF FPC}
  {$MODE ObjFPC}
  {.$MODESWITCH DuplicateLocals+}
{$ENDIF}
{$H+}

interface

uses
  SysUtils,
  AuxTypes, AuxClasses, WinSyncObjs, SharedMemoryStream, BitVector, MemVector;

{===============================================================================
    Library-specific exceptions
===============================================================================}
type
  ESMException = class(Exception);

  ESMSystemError      = class(ESMException);
  ESMInvalidValue     = class(ESMException);
  ESMLimitMismatch    = class(ESMException);
  ESMOutOfResources   = class(ESMException);
  ESMIndexOutOfBounds = class(ESMException);

{===============================================================================
    Common types and constants
===============================================================================}
{-------------------------------------------------------------------------------
    Common types and constants - public
-------------------------------------------------------------------------------}
type
  TSMClientID      = UInt16;
  TSMMessageParam  = UInt64;
  TSMMessageResult = TSMMessageParam; // must be the same type as for parameter

  TSMMessage = record
    Sender: TSMClientID;
    Param1: TSMMessageParam;
    Param2: TSMMessageParam;
    Result: TSMMessageResult;
  end;

  TSMDispatchFlag = (dfSentMessage,dfBroadcastedMessage,dfStopDispatching);

  TSMDispatchFlags = set of TSMDispatchFlag;

type
  TSMMessageEvent    = procedure(Sender: TObject; var Msg: TSMMessage; var Flags: TSMDispatchFlags) of object;
  TSMMessageCallback = procedure(Sender: TObject; var Msg: TSMMessage; var Flags: TSMDispatchFlags);

const
  SM_MAXCLIENTS_DEF  = 128;
  SM_MAXMESSAGES_DEF = 8192;  // this must be strictly larger than SM_MAXCLIENTS_DEF

  CLIENTID_BROADCAST = $FFFF;

  INFINITE = UInt32(-1);  // infinite timeout

{-------------------------------------------------------------------------------
    Common types and constants - internal
-------------------------------------------------------------------------------}
type
  TSMCrossHandle = UInt64;

  TSMMessageIndex     = Int32;
  TSMMessageFlags     = UInt32;
  TSMMessageTimestamp = Int64;

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
type
  TSMShMemClients = packed record
    Count:        Int32;
    Flags:        UInt32;
    MapOffset:    UInt32;
    ArrayOffset:  UInt32;
  end;
  PSMShMemClients = ^TSMShMemClients;

  TSMShMemMessages = packed record
    Count:        Int32;
    Flags:        UInt32;
    ArrayOffset:  UInt32;
    // linked list indices
    FirstFree:    TSMMessageIndex;
    LastFree:     TSMMessageIndex;
    FirstUsed:    TSMMessageIndex;
    LastUsed:     TSMMessageIndex;
  end;
  PSMShMemMessages = ^TSMShMemMessages;

  TSMShMemHead = packed record
    Initialized:  Boolean;
    MaxClients:   Int32;
    MaxMessages:  Int32;
    Flags:        UInt32;
    Clients:      TSMShMemClients;
    Messages:     TSMShMemMessages;
  end;
  PSMShMemHead = ^TSMShMemHead;

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
type
  TSMShMemClient = packed record
    Flags:        UInt32;
    Identifier:   TGUID;
    ProcessID:    DWORD;
    Synchronizer: TSMCrossHandle;
  end;
  PSMShMemClient = ^TSMShMemClient;

  TSMShMemMessage = packed record
    Sender:     TSMClientID;
    Recipient:  TSMClientID;
    Flags:      TSMMessageFlags;
    Timestamp:  TSMMessageTimestamp;
    P1_Result:  TSMMessageParam;  // also reused for result
    P2_Counter: TSMMessageParam;
    MasterMsg:  TSMMessageIndex;
    // linked list indices
    Index:      TSMMessageIndex;
    Prev:       TSMMessageIndex;
    Next:       TSMMessageIndex;
  end;
  PSMShMemMessage = ^TSMShMemMessage;

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
type
  TMSClientSynchonizer = record
    Assigned:     Boolean;
    Identifier:   TGUID;
    Synchronizer: TEvent;
  end;

  TMSClientSynchonizers = array of TMSClientSynchonizer;

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
type
  TMSFetchMessagesResult = (lmrSentMessage,lmrPostedMessage);

  TMSFetchMessagesResults = set of TMSFetchMessagesResult;

{===============================================================================
--------------------------------------------------------------------------------
                                 TMSMessageVector
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TMSMessageVector - class declaration
===============================================================================}
type
  TMSMessageVector = class(TMemVector)
  protected
    Function GetItem(Index: Integer): TSMShMemMessage; virtual;
    procedure SetItem(Index: Integer; Value: TSMShMemMessage); virtual;
    Function ItemCompare(Item1,Item2: Pointer): Integer; override;
  public
    constructor Create; overload;
    constructor Create(Memory: Pointer; Count: Integer); overload;
    Function First: TSMShMemMessage; reintroduce;
    Function Last: TSMShMemMessage; reintroduce;
    Function IndexOf(Item: TSMShMemMessage): Integer; reintroduce;
    Function Add(Item: TSMShMemMessage): Integer; reintroduce;
    procedure Insert(Index: Integer; Item: TSMShMemMessage); reintroduce;
    Function Remove(Item: TSMShMemMessage): Integer; reintroduce;
    Function Extract(Item: TSMShMemMessage): TSMShMemMessage; reintroduce;
    property Items[Index: Integer]: TSMShMemMessage read GetItem write SetItem; default;
  end;

{===============================================================================
--------------------------------------------------------------------------------
                              TSimpleMessageClient
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TSimpleMessageClient - class declaration
===============================================================================}
type
  TSimpleMessageClient = class(TCustomObject)
  protected
    fIsFounder:             Boolean;
    fClientID:              TSMClientID;
    fSharedMemory:          TSharedMemory;
    fShMemHead:             PSMShMemHead;
    fShMemClient:           PSMShMemClient;
    fShMemClientMap:        Pointer;
    fShMemClientArr:        Pointer;
    fShMemMessageArr:       Pointer;
    fClientMap:             TBitVectorStatic;
    fSynchronizer:          TEvent;
    fFetchedSentMessages:   TMSMessageVector;
    fFetchedPostedMessages: TMSMessageVector;
    fClientSyncs:           TMSClientSynchonizers;
    fOnMessageEvent:        TSMMessageEvent;
    fOnMessageCallback:     TSMMessageCallback;
    // geting item pointers
    Function GetClientArrayItemPtr(ClientIndex: Integer): PSMShMemClient; virtual;
    Function GetMessageArrayItemPtr(MessageIndex: TSMMessageIndex): PSMShMemMessage; virtual;
    Function GetMessageArrayNextItemPtr(MessageItem: PSMShMemMessage): PSMShMemMessage; virtual;  // does not check bounds
    // internal workings
    procedure DecoupleMessage(MessageIndex: TSMMessageIndex); virtual;                        // NL (= does not lock shared memory, but expects it to be locked)
    Function AddMessage(const Msg: TSMShMemMessage): TSMMessageIndex; virtual;                // NL
    procedure RemoveMessage(MessageIndex: TSMMessageIndex); virtual;                          // NL
    procedure ReleaseSentMessage(MessageIndex: TSMMessageIndex; Processed: Boolean); virtual; // NL
    procedure WakeClient(ClientIndex: Integer; SetFlags: UInt32); virtual;                    // NL
    procedure WakeClientsMsgSlots; virtual;                                                   // NL
    Function SendSinglecast(Recipient: TSMClientID; Param1, Param2: TSMMessageParam): TSMMessageResult; virtual;
    Function SendBroadcast(Param1, Param2: TSMMessageParam): TSMMessageResult; virtual;
    Function PostSinglecast(Recipient: TSMClientID; Param1, Param2: TSMMessageParam): Boolean; virtual;
    Function PostBroadcast(Param1, Param2: TSMMessageParam): Boolean; virtual;
    Function WaitMessages(Timeout: UInt32): Boolean; virtual;
    Function FetchMessages: TMSFetchMessagesResults; virtual;
    procedure DispatchSentMessages; virtual;
    procedure DispatchPostedMessages; virtual;
    Function InternalPeekMessages: Boolean; virtual;
    // events firing
    procedure DoMessage(var Msg: TSMMessage; var Flags: TSMDispatchFlags); virtual;
    // object init/final
    procedure Initialize(MaxClients,MaxMessages: Integer; const NameSpace: String); virtual;
    procedure Finalize; virtual;
    // some utilities
    Function LowClientIndex: Integer; virtual;
    Function HighClientIndex: Integer; virtual;
    Function CheckClientIndex(ClientIndex: Integer): Boolean; virtual;
    Function CheckClientID(ClientID: TSMClientID): Boolean; virtual;
    Function LowMessageIndex: TSMMessageIndex; virtual;
    Function HighMessageIndex: TSMMessageIndex; virtual;
    Function CheckMessageIndex(MessageIndex: TSMMessageIndex): Boolean; virtual;
  public
    constructor Create(MaxClients: Integer = SM_MAXCLIENTS_DEF; MaxMessages: Integer = SM_MAXMESSAGES_DEF; const NameSpace: String = '');
    destructor Destroy; override;
    Function SendMessage(Recipient: TSMClientID; Param1, Param2: TSMMessageParam): TSMMessageResult; virtual;
    Function PostMessage(Recipient: TSMClientID; Param1, Param2: TSMMessageParam): Boolean; virtual;
    procedure GetMessages(Timeout: UInt32 = INFINITE); virtual;
    procedure PeekMessages; virtual;
    property IsFounder: Boolean read fIsFounder;
    property ClientID: TSMClientID read fClientID;
    property OnMessageCallback: TSMMessageCallback read fOnMessageCallback write fOnMessageCallback;
    property OnMessageEvent: TSMMessageEvent read fOnMessageEvent write fOnMessageEvent;
    property OnMessage: TSMMessageEvent read fOnMessageEvent write fOnMessageEvent;
  end;

(*
threadvar
  ThreadMessageClient

InitMessages(Handler)
FinalMessages

SendMessage
PostMessage
GetMessages(Timeout)
PeekMessages
*)


implementation

uses
  Windows, Math;

{===============================================================================
    Auxiliary functions
===============================================================================}

Function GetTimestamp: TSMMessageTimestamp;
{$IFNDEF Windows}
var
  Time: TTimeSpec;
begin
If clock_gettime(CLOCK_MONOTONIC_RAW,@Time) = 0 then
  Result := (Int64(Time.tv_sec) * 1000000000) + Time.tv_nsec
else
  raise ESMSystemError.CreateFmt('GetTimestamp: Cannot obtain time stamp (%d).',[errno]);
{$ELSE}
begin
Result := 0;
If not QueryPerformanceCounter(Result) then
  raise ESMSystemError.CreateFmt('GetTimestamp: Cannot obtain time stamp (%d).',[GetLastError]);
{$ENDIF}
Result := Result and $7FFFFFFFFFFFFFFF; // mask out sign bit
end;

//------------------------------------------------------------------------------

Function GetElapsedMillis(StartTime: TSMMessageTimestamp): UInt32;
var
  CurrentTime:  TSMMessageTimestamp;
  Temp:         Int64;
begin
CurrentTime := GetTimestamp;
If CurrentTime >= StartTime then
  begin
  {$IFDEF Windows}
    Temp := 1;
    If QueryPerformanceFrequency(Temp) then
      Temp := Trunc(((CurrentTime - StartTime) / Temp) * 1000)
    else
      raise ESMSystemError.CreateFmt('GetElapsedMillis: Cannot obtain timer frequency (%d).',[GetLastError]);
  {$ELSE}
    Temp := (CurrentTime - StartTime) div 1000000;  // stamps are in ns, convert to ms
  {$ENDIF}
    If Temp < INFINITE then
      Result := UInt32(Temp)
    else
      Result := INFINITE;
  end
else Result := INFINITE;
end;


{===============================================================================
--------------------------------------------------------------------------------
                                 TMSMessageVector
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TMSMessageVector - class implementation
===============================================================================}
{-------------------------------------------------------------------------------
    TMSMessageVector - protected methods
-------------------------------------------------------------------------------}

Function TMSMessageVector.GetItem(Index: Integer): TSMShMemMessage;
begin
Result := TSMShMemMessage(GetItemPtr(Index)^);
end;

//------------------------------------------------------------------------------

procedure TMSMessageVector.SetItem(Index: Integer; Value: TSMShMemMessage);
begin
SetItemPtr(Index,@Value);
end;

//------------------------------------------------------------------------------

Function TMSMessageVector.ItemCompare(Item1,Item2: Pointer): Integer;
begin
{
  Order messages only by time.
  
  Because messages are traversed from high index to low, the sorting is done in
  reverse (from higher timestamp to lower, so the older are dispatched first).
}
If TSMShMemMessage(Item1^).Timestamp < TSMShMemMessage(Item2^).Timestamp then
  Result := +1
else If TSMShMemMessage(Item1^).Timestamp > TSMShMemMessage(Item2^).Timestamp then
  Result := -1
else
  Result := 0;
end;

{-------------------------------------------------------------------------------
    TMSMessageVector - public methods
-------------------------------------------------------------------------------}

constructor TMSMessageVector.Create;
begin
inherited Create(SizeOf(TSMShMemMessage));
ShrinkMode := smKeepCap;
end;

//   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---

constructor TMSMessageVector.Create(Memory: Pointer; Count: Integer);
begin
inherited Create(Memory,Count,SizeOf(TSMShMemMessage));
end;

//------------------------------------------------------------------------------

Function TMSMessageVector.First: TSMShMemMessage;
begin
Result := TSMShMemMessage(inherited First^);
end;

//------------------------------------------------------------------------------

Function TMSMessageVector.Last: TSMShMemMessage;
begin
Result := TSMShMemMessage(inherited Last^);
end;

//------------------------------------------------------------------------------

Function TMSMessageVector.IndexOf(Item: TSMShMemMessage): Integer;
begin
Result := inherited IndexOf(@Item);
end;

//------------------------------------------------------------------------------

Function TMSMessageVector.Add(Item: TSMShMemMessage): Integer;
begin
Result := inherited Add(@Item);
end;

//------------------------------------------------------------------------------

procedure TMSMessageVector.Insert(Index: Integer; Item: TSMShMemMessage);
begin
inherited Insert(Index,@Item);
end;

//------------------------------------------------------------------------------

Function TMSMessageVector.Remove(Item: TSMShMemMessage): Integer;
begin
Result := inherited Remove(@Item);
end;

//------------------------------------------------------------------------------

Function TMSMessageVector.Extract(Item: TSMShMemMessage): TSMShMemMessage;
var
  TempPtr:  Pointer;
begin
TempPtr := inherited Extract(@Item);
If Assigned(TempPtr) then
  Result := TSMShMemMessage(TempPtr^)
else
  FillChar(Addr(Result)^,SizeOf(Result),0);
end;


{===============================================================================
--------------------------------------------------------------------------------
                              TSimpleMessageClient
--------------------------------------------------------------------------------
===============================================================================}
const
  SM_SHAREDMEM_PREFIX = 'sm_shared_mem_';

  SM_CLIENTSFLAG_MSGSLTWT = UInt32($00000001);  // at least one client is waiting for a free message slot

  SM_CLIENTFLAG_RECVSMSG = UInt32($00000001); // received sent message(s)
  SM_CLIENTFLAG_RECVPMSG = UInt32($00000002); // received posted message(s)
  SM_CLIENTFLAG_RECVMSG  = UInt32(SM_CLIENTFLAG_RECVSMSG or SM_CLIENTFLAG_RECVPMSG);  // received a message
  SM_CLIENTFLAG_MSGSLTWT = UInt32($00000004); // client is waiting for a free message slot

  SM_MSGFLAG_SENT       = UInt32($00000001);  // message was sent, not posted
  SM_MSGFLAG_RELEASED   = UInt32($00000002);  // sender is released from waiting
  SM_MSGFLAG_PROCESSED  = UInt32($00000003);  // message was processed by recipient
  SM_MSGFLAG_BROADCAST  = UInt32($00000004);  // broadcasted message
  SM_MSGFLAG_STBCMASTER = UInt32($00000008);  // sent broadcasted master message

{===============================================================================
    TSimpleMessageClient - class implementation
===============================================================================}
{-------------------------------------------------------------------------------
    TSimpleMessageClient - protected methods
-------------------------------------------------------------------------------}

Function TSimpleMessageClient.GetClientArrayItemPtr(ClientIndex: Integer): PSMShMemClient;
begin
If CheckClientIndex(ClientIndex) then
  Result := PSMShMemClient(PtrUInt(fShMemClientArr) + PtrUInt(ClientIndex * SizeOf(TSMShMemClient)))
else
  raise ESMIndexOutOfBounds.CreateFmt('TSimpleMessageClient.GetClientArrayItemPtr: Index (%d) out of bounds.',[ClientIndex]);
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.GetMessageArrayItemPtr(MessageIndex: TSMMessageIndex): PSMShMemMessage;
begin
If CheckMessageIndex(MessageIndex) then
  Result := PSMShMemMessage(PtrUInt(fShMemMessageArr) + PtrUInt(MessageIndex * SizeOf(TSMShMemMessage)))
else
  raise ESMIndexOutOfBounds.CreateFmt('TSimpleMessageClient.GetMessageArrayItemPtr: Index (%d) out of bounds.',[MessageIndex]);
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.GetMessageArrayNextItemPtr(MessageItem: PSMShMemMessage): PSMShMemMessage;
begin
Result := PSMShMemMessage(PtrUInt(MessageItem) + PtrUInt(SizeOf(TSMShMemMessage)));
end;

//------------------------------------------------------------------------------

procedure TSimpleMessageClient.DecoupleMessage(MessageIndex: TSMMessageIndex);
var
  MessageItemPtr: PSMShMemMessage;
begin
If CheckMessageIndex(MessageIndex) then
  begin
    MessageItemPtr := GetMessageArrayItemPtr(MessageIndex);
    If CheckMessageIndex(MessageItemPtr^.Prev) then
      GetMessageArrayItemPtr(MessageItemPtr^.Prev).Next := MessageItemPtr^.Next;
    If CheckMessageIndex(MessageItemPtr^.Next) then
      GetMessageArrayItemPtr(MessageItemPtr^.Next).Prev := MessageItemPtr^.Prev;
    If MessageIndex = fShMemHead^.Messages.FirstFree then
      fShMemHead^.Messages.FirstFree := MessageItemPtr^.Next;
    If MessageIndex = fShMemHead^.Messages.LastFree then
      fShMemHead^.Messages.LastFree := MessageItemPtr^.Prev;
    If MessageIndex = fShMemHead^.Messages.FirstUsed then
      fShMemHead^.Messages.FirstUsed := MessageItemPtr^.Next;
    If MessageIndex = fShMemHead^.Messages.LastUsed then
      fShMemHead^.Messages.LastUsed := MessageItemPtr^.Prev;
    MessageItemPtr^.Prev := -1;
    MessageItemPtr^.Next := -1;
  end
else raise ESMIndexOutOfBounds.CreateFmt('TSimpleMessageClient.DecoupleMessage: Message index (%d) out of bounds.',[MessageIndex]);
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.AddMessage(const Msg: TSMShMemMessage): TSMMessageIndex;
var
  MessageItemPtr: PSMShMemMessage;
begin
If CheckMessageIndex(fShMemHead^.Messages.FirstFree) then
  begin
    Result := fShMemHead^.Messages.FirstFree;
    MessageItemPtr := GetMessageArrayItemPtr(Result);
    // first move the item from free to used list
    DecoupleMessage(MessageItemPtr^.Index);
    If not CheckMessageIndex(fShMemHead^.Messages.FirstUsed) then
      fShMemHead^.Messages.FirstUsed := MessageItemPtr^.Index;
    If CheckMessageIndex(fShMemHead^.Messages.LastUsed) then
      GetMessageArrayItemPtr(fShMemHead^.Messages.LastUsed)^.Next := MessageItemPtr^.Index;
    MessageItemPtr^.Prev := fShMemHead^.Messages.LastUsed;
    MessageItemPtr^.Next := -1;
    fShMemHead^.Messages.LastUsed := MessageItemPtr^.Index;
    // now copy the data
    MessageItemPtr^.Sender := Msg.Sender;
    MessageItemPtr^.Recipient := Msg.Recipient;
    MessageItemPtr^.Flags := Msg.Flags;
    MessageItemPtr^.Timestamp := Msg.Timestamp;
    MessageItemPtr^.P1_Result := Msg.P1_Result;
    MessageItemPtr^.P2_Counter := Msg.P2_Counter;
    MessageItemPtr^.MasterMsg := Msg.MasterMsg;
    Inc(fShMemHead^.Messages.Count);
  end
else raise ESMOutOfResources.Create('TSimpleMessageClient.AddMessage: No free message slot.');
end;

//------------------------------------------------------------------------------

procedure TSimpleMessageClient.RemoveMessage(MessageIndex: TSMMessageIndex);
var
  MessageItemPtr: PSMShMemMessage;
begin
If CheckMessageIndex(MessageIndex) then
  begin
    MessageItemPtr := GetMessageArrayItemPtr(MessageIndex);
    // just move the item to the list of free
    DecoupleMessage(MessageIndex);
    If not CheckMessageIndex(fShMemHead^.Messages.FirstFree) then
      fShMemHead^.Messages.FirstFree := MessageIndex;
    If CheckMessageIndex(fShMemHead^.Messages.LastFree) then
      GetMessageArrayItemPtr(fShMemHead^.Messages.LastFree)^.Next := MessageIndex;
    MessageItemPtr^.Prev := fShMemHead^.Messages.LastFree;
    MessageItemPtr^.Next := -1;
    fShMemHead^.Messages.LastFree := MessageIndex;
    Dec(fShMemHead^.Messages.Count);
  end
else raise ESMIndexOutOfBounds.CreateFmt('TSimpleMessageClient.RemoveMessage: Message index (%d) out of bounds.',[MessageIndex]);
end;

//------------------------------------------------------------------------------

procedure TSimpleMessageClient.WakeClient(ClientIndex: Integer; SetFlags: UInt32);
var
  ClientItemPtr:  PSMShMemClient;

  procedure DuplicateClientSynchronizer;
  begin
    fClientSyncs[ClientIndex].Assigned := True;
    fClientSyncs[ClientIndex].Identifier := ClientItemPtr^.Identifier;
    fClientSyncs[ClientIndex].Synchronizer :=
      TEvent.DuplicateFromProcessID(ClientItemPtr^.ProcessID,THandle(ClientItemPtr^.Synchronizer));
  end;

begin
If CheckClientIndex(ClientIndex) then
  begin
    ClientItemPtr := GetClientArrayItemPtr(ClientIndex);
    If fClientSyncs[ClientIndex].Assigned then
      begin
        If not IsEqualGUID(fClientSyncs[ClientIndex].Identifier,ClientItemPtr^.Identifier) then
          begin
            fClientSyncs[ClientIndex].Synchronizer.Free;
            DuplicateClientSynchronizer;
          end;
      end
    else DuplicateClientSynchronizer;
    // set flags and release the event
    ClientItemPtr^.Flags := ClientItemPtr^.Flags or SetFlags;
    fClientSyncs[ClientIndex].Synchronizer.SetEventStrict;
  end
else raise ESMIndexOutOfBounds.CreateFmt('TSimpleMessageClient.WakeClient: Client index (%d) out of bounds.',[ClientIndex]);
end;

//------------------------------------------------------------------------------

procedure TSimpleMessageClient.WakeClientsMsgSlots;
var
  FreeMsgSlots: Integer;
  i:            Integer;
begin
// wake clients waiting for free message slot (as many as there is free slots)
If fShMemHead^.Clients.Flags and SM_CLIENTSFLAG_MSGSLTWT <> 0 then
  begin
    FreeMsgSlots := fShMemHead^.MaxMessages - fShMemHead^.Messages.Count;
    If FreeMsgSlots > 0 then
      For i := LowClientIndex to HighClientIndex do
        If fClientMap[i] and (GetClientArrayItemPtr(i)^.Flags and SM_CLIENTFLAG_MSGSLTWT <> 0) then
          begin
            WakeClient(TSMClientID(i),0);
            If FreeMsgSlots > 1 then
              Dec(FreeMsgSlots)
            else
              Break{For i};
          end;
  end;
end;

//------------------------------------------------------------------------------

procedure TSimpleMessageClient.ReleaseSentMessage(MessageIndex: TSMMessageIndex; Processed: Boolean);
var
  MessageItemPtr: PSMShMemMessage;
  MasterMessage:  PSMShMemMessage;
begin
If CheckMessageIndex(MessageIndex) then
  begin
    MessageItemPtr := GetMessageArrayItemPtr(MessageIndex);
    If MessageItemPtr^.Flags and SM_MSGFLAG_SENT <> 0 then
      begin
        If MessageItemPtr^.Flags and SM_MSGFLAG_BROADCAST <> 0 then
          begin
            // broadcasted message
            If CheckMessageIndex(MessageItemPtr^.MasterMsg) then
              begin
                MasterMessage := GetMessageArrayItemPtr(MessageItemPtr^.MasterMsg);
                If MasterMessage^.Flags and SM_MSGFLAG_STBCMASTER <> 0 then
                  begin
                    If Processed then
                      MasterMessage^.Flags := MasterMessage^.Flags or SM_MSGFLAG_PROCESSED;
                    MasterMessage^.P1_Result := MessageItemPtr^.P1_Result;
                    MasterMessage^.P2_Counter := UInt64(Int64(MasterMessage^.P2_Counter) - 1);
                    If Int64(MasterMessage^.P2_Counter) <= 0 then
                      begin
                        MasterMessage^.Flags := MasterMessage^.Flags or SM_MSGFLAG_RELEASED;
                        WakeClient(Integer(MasterMessage^.Sender),0);
                      end;
                  end;
              end;
            RemoveMessage(MessageIndex);  // remove the submessage
          end
        else
          begin
            // singlecast message
            MessageItemPtr^.Flags := MessageItemPtr^.Flags or SM_MSGFLAG_RELEASED;
            If Processed then
              MessageItemPtr^.Flags := MessageItemPtr^.Flags or SM_MSGFLAG_PROCESSED;
            WakeClient(Integer(MessageItemPtr^.Sender),0);
          end;
      end;
  end
else raise ESMIndexOutOfBounds.CreateFmt('TSimpleMessageClient.ReleaseMessage: Message index (%d) out of bounds.',[MessageIndex]);
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.SendSinglecast(Recipient: TSMClientID; Param1, Param2: TSMMessageParam): TSMMessageResult;
//var
//  TempMessage:  TSMShMemMessage;
//  MessageIndex: TSMMessageIndex;
begin
Result := 0;
(*
fSharedMemory.Lock;
try
  If fClientMap[Integer(Recipient)] and (fShMemHead^.Messages.Count < fShMemHead^.MaxMessages) then
    begin
      TempMessage.Sender := fClientID;
      TempMessage.Recipient := Recipient;
      TempMessage.Flags := SM_MSGFLAG_SENT;
      TempMessage.Timestamp := GetTimestamp;
      TempMessage.Param1 := Param1;
      TempMessage.Param2 := Param2;
      TempMessage.LinkedMsg := -1;
      MessageIndex := AddMessage(TempMessage);
      WakeClient(Recipient,SM_CLIENTFLAG_RECVSMSG);
      fSharedMemory.Unlock;
      //self.fSynchronizer.wa
      fSharedMemory.Lock;
      Result := TSMMessageResult(GetMessageArrayItemPtr(MessageIndex)^.Param2);
      RemoveMessage(MessageIndex);
    end;
finally
  fSharedMemory.Unlock;
end;
*)
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.SendBroadcast(Param1, Param2: TSMMessageParam): TSMMessageResult;
begin
{$message 'todo'}
Result := 0;
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.PostSinglecast(Recipient: TSMClientID; Param1, Param2: TSMMessageParam): Boolean;
var
  TempMessage:  TSMShMemMessage;
begin
Result := False;
fSharedMemory.Lock;
try
  // continue only if the recipient exists and there is a room for the message
  If fClientMap[Integer(Recipient)] and (fShMemHead^.Messages.Count < fShMemHead^.MaxMessages) then
    begin
      TempMessage.Sender := fClientID;
      TempMessage.Recipient := Recipient;
      TempMessage.Flags := 0;
      TempMessage.Timestamp := GetTimestamp;
      TempMessage.P1_Result := Param1;
      TempMessage.P2_Counter := Param2;
      TempMessage.MasterMsg := -1;
      AddMessage(TempMessage);
      WakeClient(Integer(Recipient),SM_CLIENTFLAG_RECVPMSG);
      Result := True;
    end;
finally
  fSharedMemory.Unlock;
end;
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.PostBroadcast(Param1, Param2: TSMMessageParam): Boolean;
var
  TempMessage:  TSMShMemMessage;
  i:            Integer;
begin
Result := False;
fSharedMemory.Lock;
try
  If (fShMemHead^.Clients.Count > 0) and
     (fShMemHead^.Clients.Count <= (fShMemHead^.MaxMessages - fShMemHead^.Messages.Count)) then
    begin
      TempMessage.Sender := fClientID;
      TempMessage.Flags := SM_MSGFLAG_BROADCAST;
      TempMessage.Timestamp := GetTimestamp;
      TempMessage.P1_Result := Param1;
      TempMessage.P2_Counter := Param2;
      TempMessage.MasterMsg := -1;
      For i := LowClientIndex to HighClientIndex do
        If fClientMap[i] then // does this client exist?
          begin
            TempMessage.Recipient := TSMClientID(i);
            AddMessage(TempMessage);
            WakeClient(Integer(TempMessage.Recipient),SM_CLIENTFLAG_RECVPMSG);
          end;
      Result := True;
    end;
finally
  fSharedMemory.Unlock;
end;
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.WaitMessages(Timeout: UInt32): Boolean;

  Function CheckMessages: Boolean;
  begin
    fSharedMemory.Lock;
    try
      Result := fShMemClient^.Flags and SM_CLIENTFLAG_RECVMSG <> 0;
    finally
      fSharedMemory.Unlock;
    end;
  end;

var
  StartTime:        TSMMessageTimestamp;
  TimeoutRemaining: UInt32;
  ElapsedMillis:    UInt32;
  ExitWait:         Boolean;
begin
Result := True;
StartTime := GetTimestamp;
TimeoutRemaining := Timeout;
If (fFetchedSentMessages.Count <= 0) and (fFetchedPostedMessages.Count <= 0) then
  repeat
    ExitWait := True;
    If fSynchronizer.WaitFor(TimeoutRemaining) in [wrSignaled,wrAbandoned,wrIOCompletion,wrMessage] then
      begin
        If not CheckMessages then
          begin
            // no message received, recalculate timeout and re-enter waiting
            If Timeout <> INFINITE then
              begin
                ElapsedMillis := GetElapsedMillis(StartTime);
                If Timeout <= ElapsedMillis then
                  begin
                    Result := False;
                    Break{repeat};
                  end
                else TimeoutRemaining := Timeout - ElapsedMillis;
              end;
            ExitWait := False;
          end;
      end
    else Result := False;
  until ExitWait;
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.FetchMessages: TMSFetchMessagesResults;
var
  MessageIndex:   TSMMessageIndex;
  MessageItemPtr: PSMShMemMessage;
begin
fSharedMemory.Lock;
try
  If fShMemClient^.Flags and SM_CLIENTFLAG_RECVMSG <> 0 then
    begin
      MessageIndex := fShMemHead^.Messages.FirstUsed;
      while CheckMessageIndex(MessageIndex) do
        begin
          MessageItemPtr := GetMessageArrayItemPtr(MessageIndex);
          MessageIndex := MessageItemPtr^.Next;
          If MessageItemPtr^.Recipient = fClientID then
            begin
              If MessageItemPtr^.Flags and SM_MSGFLAG_SENT = 0 then
                begin
                  // posted message
                  fFetchedPostedMessages.Add(MessageItemPtr^);
                  RemoveMessage(MessageItemPtr^.Index);
                end
              else fFetchedSentMessages.Add(MessageItemPtr^); // sent mesages are not deleted
            end;
        end;
      fShMemClient^.Flags := fShMemClient^.Flags and not SM_CLIENTFLAG_RECVMSG;
      WakeClientsMsgSlots;
    end;
finally
  fSharedMemory.Unlock;
end;
fFetchedSentMessages.Sort;
fFetchedPostedMessages.Sort;
Result := [];
If fFetchedSentMessages.Count > 0 then
  Include(Result,lmrSentMessage);
If fFetchedPostedMessages.Count > 0 then
  Include(Result,lmrPostedMessage);
end;

//------------------------------------------------------------------------------

procedure TSimpleMessageClient.DispatchSentMessages;
var
  TempShMemMessage: TSMShMemMessage;
  TempMessage:      TSMMessage;
  DispatchFlags:    TSMDispatchFlags;
  i:                Integer;
begin
exit;
If Assigned(fOnMessageEvent) or Assigned(fOnMessageCallback) then
  begin
    while fFetchedSentMessages.Count > 0 do
      begin
        TempShMemMessage := fFetchedSentMessages[fFetchedSentMessages.HighIndex];
        fFetchedSentMessages.Delete(fFetchedSentMessages.HighIndex);
        TempMessage.Sender := TempShMemMessage.Sender;
        TempMessage.Param1 := TempShMemMessage.P1_Result;
        TempMessage.Param2 := TempShMemMessage.P2_Counter;
        TempMessage.Result := 0;
        DispatchFlags := [dfSentMessage];
        If TempShMemMessage.Flags and SM_MSGFLAG_BROADCAST <> 0 then
          Include(DispatchFlags,dfBroadcastedMessage);
        DoMessage(TempMessage,DispatchFlags); // <<<
        fSharedMemory.Lock;
        try
          GetMessageArrayItemPtr(TempShMemMessage.Index)^.P1_Result := TempMessage.Result;
          ReleaseSentMessage(TempShMemMessage.Index,True);
        finally
          fSharedMemory.Unlock;
        end;
        If dfStopDispatching in DispatchFlags then
          Break{while};
      end;
  end
else
  begin
    fSharedMemory.Lock;
    try
      For i := fFetchedSentMessages.LowIndex to fFetchedSentMessages.HighIndex do
        ReleaseSentMessage(fFetchedSentMessages[i].Index,False);
    finally
      fSharedMemory.Unlock;
    end;
    fFetchedSentMessages.Clear;
  end;
end;

//------------------------------------------------------------------------------

procedure TSimpleMessageClient.DispatchPostedMessages;
var
  TempShMemMessage: TSMShMemMessage;
  TempMessage:      TSMMessage;
  DispatchFlags:    TSMDispatchFlags;
begin
If Assigned(fOnMessageEvent) or Assigned(fOnMessageCallback) then
  begin
    while fFetchedPostedMessages.Count > 0 do
      begin
        TempShMemMessage := fFetchedPostedMessages[fFetchedPostedMessages.HighIndex];
        fFetchedPostedMessages.Delete(fFetchedPostedMessages.HighIndex);
        TempMessage.Sender := TempShMemMessage.Sender;
        TempMessage.Param1 := TempShMemMessage.P1_Result;
        TempMessage.Param2 := TempShMemMessage.P2_Counter;
        TempMessage.Result := 0;
        DispatchFlags := [];
        If TempShMemMessage.Flags and SM_MSGFLAG_BROADCAST <> 0 then
          Include(DispatchFlags,dfBroadcastedMessage);
        DoMessage(TempMessage,DispatchFlags); // <<<
        If dfStopDispatching in DispatchFlags then
          Break{while};
      end;
  end
else fFetchedPostedMessages.Clear;
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.InternalPeekMessages: Boolean;
begin
If FetchMessages <> [] then
  begin
    DispatchSentMessages;
    DispatchPostedMessages;
    Result := True;
  end
else Result := False;
end;

//------------------------------------------------------------------------------

procedure TSimpleMessageClient.DoMessage(var Msg: TSMMessage; var Flags: TSMDispatchFlags);
begin
If Assigned(fOnMessageEvent) then
  fOnMessageEvent(Self,Msg,Flags)
else If Assigned(fOnMessageCallback) then
  fOnMessageCallback(Self,Msg,Flags);
end;

//------------------------------------------------------------------------------

procedure TSimpleMessageClient.Initialize(MaxClients,MaxMessages: Integer; const NameSpace: String);
var
  MapFreeIdx:     Integer;
  MessageItemPtr: PSMShMemMessage;
  i:              TSMMessageIndex;
  j:              Integer;
begin
// sanity checks
If (MaxClients <= 0) or (MaxClients >= CLIENTID_BROADCAST) then
  raise ESMInvalidValue.CreateFmt('TSimpleMessageClient.Initialize: Invalid client limit (%d).',[MaxClients]);
If (MaxMessages <= 0) or (MaxMessages <= MaxClients) then
  raise ESMInvalidValue.CreateFmt('TSimpleMessageClient.Initialize: Invalid message limit (%d).',[MaxMessages]);
// create shared memory
fSharedMemory := TSharedMemory.Create(
  // calculate expected shared memory size...
  (TMemSize(SizeOf(TSMShMemHead) + 31) and not TMemSize(31)) +                  // head
  (TMemSize(Ceil(MaxClients / 8) + 31) and not TMemSize(31)) +                  // client map
  (TMemSize((MaxClients * SizeOf(TSMShMemClient)) + 31) and not TMemSize(31)) + // client array
  (TMemSize(MaxMessages * SizeOf(TSMShMemMessage)))                             // message array
  ,SM_SHAREDMEM_PREFIX + NameSpace);
fSharedMemory.Lock;
try
  fShMemHead := PSMShMemHead(fSharedMemory.Memory);
  If fShMemHead^.Initialized then
    begin
      fIsFounder := False;
      // not a first access to this memory, check limits
      If fShMemHead^.MaxClients <> MaxClients then
        raise ESMLimitMismatch.CreateFmt('TSimpleMessageClient.Initialize: Client limit does not match (%d/%d).',
          [MaxClients,fShMemHead^.MaxClients]);
      If fShMemHead^.MaxMessages <> MaxMessages then
        raise ESMLimitMismatch.CreateFmt('TSimpleMessageClient.Initialize: Message limit does not match (%d/%d).',
          [MaxMessages,fShMemHead^.MaxMessages]);
    end
  else
    begin
      fIsFounder := True;
      // first access to this memory, initialize everything
      fShMemHead^.Initialized := True;
      fShMemHead^.MaxClients := MaxClients;
      fShMemHead^.MaxMessages := MaxMessages;
      fShMemHead^.Clients.MapOffset := TMemSize(SizeOf(TSMShMemHead) + 31) and not TMemSize(31);
      fShMemHead^.Clients.ArrayOffset := fShMemHead^.Clients.MapOffset +
        (TMemSize(Ceil(MaxClients / 8) + 31) and not TMemSize(31));
      fShMemHead^.Messages.ArrayOffset := fShMemHead^.Clients.ArrayOffset +
        (TMemSize((MaxClients * SizeOf(TSMShMemClient)) + 31) and not TMemSize(31));
    end;
  // calculate pointers from offsets
  fShMemClientMap := Pointer(PtrUInt(fShMemHead) + PtrUInt(fShMemHead^.Clients.MapOffset));
  fShMemClientArr := Pointer(PtrUInt(fShMemHead) + PtrUInt(fShMemHead^.Clients.ArrayOffset));
  fShMemMessageArr := Pointer(PtrUInt(fShMemHead) + PtrUInt(fShMemHead^.Messages.ArrayOffset));
  // init the linked list (must be after pointers setup)
  If fIsFounder then
    begin
      fShMemHead^.Messages.FirstFree := LowMessageIndex;
      fShMemHead^.Messages.LastFree := HighMessageIndex;
      fShMemHead^.Messages.FirstUsed := -1;
      fShMemHead^.Messages.LastUsed := -1;
      MessageItemPtr := GetMessageArrayItemPtr(0);
      For i := LowMessageIndex to HighMessageIndex do
        begin
          MessageItemPtr^.Index := i;
          If i <= LowMessageIndex then
            MessageItemPtr^.Prev := -1
          else
            MessageItemPtr^.Prev := Pred(i);
          If i >= HighMessageIndex then
            MessageItemPtr^.Next := -1
          else
            MessageItemPtr^.Next := Succ(i);
          MessageItemPtr := GetMessageArrayNextItemPtr(MessageItemPtr);
        end;
    end;
  // add self to the client array
  fClientMap := TBitVectorStatic.Create(fShMemClientMap,MaxClients);
  MapFreeIdx := fClientMap.FirstClean;
  If fClientMap.CheckIndex(MapFreeIdx) then
    begin
      fSynchronizer := TEvent.Create(False,False);
      fShMemClient := GetClientArrayItemPtr(MapFreeIdx);
      fShMemClient^.Flags := 0;
      If CreateGUID(fShMemClient^.Identifier) <> S_OK then
        raise ESMOutOfResources.Create('TSimpleMessageClient.Initialize: Cannot generate client GUID.');
      fShMemClient^.ProcessID := GetCurrentProcessID;
      fShMemClient^.Synchronizer := TSMCrossHandle(fSynchronizer.Handle);
      Inc(fShMemHead^.Clients.Count);
      fClientMap[MapFreeIdx] := True;
      fClientID := TSMClientID(MapFreeIdx);
    end
  else raise ESMOutOfResources.Create('TSimpleMessageClient.Initialize: No free client slot.');
  // client synchronizers (make sure they are properly initialized)
  SetLength(fClientSyncs,MaxClients);
  For j := LowClientIndex to HighClientIndex do
    If j <> Integer(fClientID) then
      begin
        fClientSyncs[j].Assigned := False;
        FillChar(fClientSyncs[j].Identifier,SizeOf(TGUID),0);
        fClientSyncs[j].Synchronizer := nil;
      end
    else
      begin
        fClientSyncs[j].Assigned := True;
        fClientSyncs[j].Identifier := fShMemClient^.Identifier;
        fClientSyncs[j].Synchronizer := fSynchronizer;
      end;
  // vectors for received messages
  fFetchedSentMessages := TMSMessageVector.Create;
  fFetchedPostedMessages := TMSMessageVector.Create;
finally
  fSharedMemory.Unlock;
end;
end;

//------------------------------------------------------------------------------

procedure TSimpleMessageClient.Finalize;
var
  i:  Integer;
begin
fSharedMemory.Lock;
try
  // get and release all sent messages (also remove the posted ones)
  FetchMessages;
  For i := fFetchedSentMessages.LowIndex to fFetchedSentMessages.HighIndex do
    ReleaseSentMessage(fFetchedSentMessages[i].Index,False);
  // free message vectors
  fFetchedPostedMessages.Free;
  fFetchedSentMessages.Free;
  // free synchronizers
  For i := LowClientIndex to HighClientIndex do
    If fClientSyncs[i].Assigned and (i <> Integer(fClientID)) then
      fClientSyncs[i].Synchronizer.Free;
  // remove self from clients
  fClientMap[Integer(fClientID)] := False;
  Dec(fShMemHead^.Clients.Count);
  // cleanup
  fSynchronizer.Free;
  fClientMap.Free;
finally
  fSharedMemory.Unlock;
end;
fSharedMemory.Free;
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.LowClientIndex: Integer;
begin
Result := 0;
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.HighClientIndex: Integer;
begin
Result := Pred(fShMemHead^.MaxClients);
end;
 
//------------------------------------------------------------------------------

Function TSimpleMessageClient.CheckClientIndex(ClientIndex: Integer): Boolean;
begin
Result := (ClientIndex >= LowClientIndex) and (ClientIndex <= HighClientIndex);
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.CheckClientID(ClientID: TSMClientID): Boolean;
begin
Result := CheckClientIndex(Integer(ClientID)) or (ClientID = CLIENTID_BROADCAST);
end;
 
//------------------------------------------------------------------------------

Function TSimpleMessageClient.LowMessageIndex: TSMMessageIndex;
begin
Result := 0;
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.HighMessageIndex: TSMMessageIndex;
begin
Result := Pred(fShMemHead^.MaxMessages);
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.CheckMessageIndex(MessageIndex: TSMMessageIndex): Boolean;
begin
Result := (MessageIndex >= LowMessageIndex) and (MessageIndex <= HighMessageIndex);
end;

{-------------------------------------------------------------------------------
    TSimpleMessageClient - public methods
-------------------------------------------------------------------------------}

constructor TSimpleMessageClient.Create(MaxClients: Integer = SM_MAXCLIENTS_DEF; MaxMessages: Integer = SM_MAXMESSAGES_DEF; const NameSpace: String = '');
begin
inherited Create;
Initialize(MaxClients,MaxMessages,NameSpace);
end;

//------------------------------------------------------------------------------

destructor TSimpleMessageClient.Destroy;
begin
Finalize;
inherited;
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.SendMessage(Recipient: TSMClientID; Param1, Param2: TSMMessageParam): TSMMessageResult;
begin
If CheckClientID(Recipient) then
  begin
    If Recipient = CLIENTID_BROADCAST then
      Result := SendBroadcast(Param1,Param2)
    else
      Result := SendSinglecast(Recipient,Param1,Param2);
  end
else Result := 0;
end;

//------------------------------------------------------------------------------

Function TSimpleMessageClient.PostMessage(Recipient: TSMClientID; Param1, Param2: TSMMessageParam): Boolean;
begin
If CheckClientID(Recipient) then
  begin
    If Recipient = CLIENTID_BROADCAST then
      Result := PostBroadcast(Param1,Param2)
    else
      Result := PostSinglecast(Recipient,Param1,Param2);
  end
else Result := False;
end;

//------------------------------------------------------------------------------

procedure TSimpleMessageClient.GetMessages(Timeout: UInt32 = INFINITE);
begin
// enter waiting only if there is no message already received
If not InternalPeekMessages then
  If WaitMessages(Timeout) then
    InternalPeekMessages;
end;

//------------------------------------------------------------------------------

procedure TSimpleMessageClient.PeekMessages;
begin
InternalPeekMessages;
end;


end.
