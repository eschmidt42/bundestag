import typing as T

from pydantic import BaseModel


class FieldLegislature(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str
    abgeordnetenwatch_url: str


class FieldTopic(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str
    abgeordnetenwatch_url: str


class FieldCommittee(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str


class RelatedLink(BaseModel):
    uri: str
    title: str


class Poll(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str
    field_legislature: FieldLegislature
    field_topics: T.List[FieldTopic]
    field_committees: T.List[FieldCommittee] = None
    field_intro: str
    field_poll_date: str
    field_related_links: T.List[RelatedLink] = None


class AbgeordnetenwatchAPI(BaseModel):
    version: str
    changelog: str
    licence: str
    licence_link: str
    documentation: str


class PollResult(BaseModel):
    count: int
    total: int
    range_start: int
    range_end: int


class AbgeordnetenwatchMeta(BaseModel):
    abgeordnetenwatch_api: AbgeordnetenwatchAPI
    status: str
    status_message: str
    result: PollResult


class PollResponse(BaseModel):
    meta: AbgeordnetenwatchMeta
    data: T.List[Poll]


class ParliamentPeriod(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str
    abgeordnetenwatch_url: str


class Politician(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str
    abgeordnetenwatch_url: str


class ElectoralList(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str


class Constituency(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str


class ElectoralData(BaseModel):
    id: int
    entity_type: str
    label: str
    electoral_list: ElectoralList = None
    list_position: int = None
    constituency: Constituency = None
    constituency_result: float = None
    constituency_result_count: float = None
    mandate_won: str = None


class Fraction(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str


class FractionMembership(BaseModel):
    id: int
    entity_type: str
    label: str
    fraction: Fraction
    valid_from: str = None
    valid_until: str = None


class Mandate(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str
    id_external_administration: str
    id_external_administration_description: str
    type: str
    parliament_period: ParliamentPeriod
    politician: Politician
    start_date: str = None
    end_date: str = None
    info: str = None
    electoral_data: ElectoralData
    fraction_membership: T.List[FractionMembership]


class MandatesResponse(BaseModel):
    meta: AbgeordnetenwatchMeta
    data: T.List[Mandate]


class VoteMandate(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str


class VotePoll(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str


class Vote(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str
    mandate: VoteMandate
    poll: VotePoll
    vote: str
    reason_no_show: str = None
    reason_no_show_other: str = None
    fraction: Fraction


class RelatedData(BaseModel):
    votes: T.List[Vote]


class VoteData(BaseModel):
    id: int
    entity_type: str
    label: str
    api_url: str
    field_legislature: FieldLegislature
    field_topics: T.List[FieldTopic]
    field_committees: T.List[FieldCommittee] = None
    field_intro: str
    field_poll_date: str
    field_related_links: T.List[RelatedLink] = None
    related_data: RelatedData


class VoteMetaResult(BaseModel):
    entity_id: str
    entity_type: str


class VoteMeta(BaseModel):
    abgeordnetenwatch_api: AbgeordnetenwatchAPI
    status: str
    status_message: str
    result: VoteMetaResult


class VoteResponse(BaseModel):
    meta: VoteMeta
    data: VoteData
