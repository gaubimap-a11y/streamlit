from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from src.core import audit
from src.core.exceptions import BusinessRuleError
from src.domain.report_filters import (
    FilterApplyResult,
    SaveFilterCommand,
    SavedFilterList,
    normalize_report_filter_payload,
    parse_report_filter_payload,
)
from src.infrastructure.databricks.client import databricks_connection
from src.infrastructure.repositories.report_filter_definition_repository import (
    ReportFilterDefinitionRepository,
)
from src.infrastructure.repositories.report_filter_share_repository import (
    ReportFilterShareRepository,
)
from src.infrastructure.repositories.user_repository import UserRepository


@dataclass(slots=True)
class ShareFilterResult:
    invalid_recipient_user_ids: list[str]


class ReportFilterService:
    def __init__(
        self,
        definition_repository: ReportFilterDefinitionRepository | None = None,
        share_repository: ReportFilterShareRepository | None = None,
        user_repository: UserRepository | None = None,
    ) -> None:
        self._definition_repository = definition_repository or ReportFilterDefinitionRepository()
        self._share_repository = share_repository or ReportFilterShareRepository()
        self._user_repository = user_repository or UserRepository()

    def resolve_user_id(self, username: str) -> str | None:
        username = str(username or "").strip().lower()
        if not username:
            return None
        with databricks_connection() as conn:
            user = self._user_repository.find_by_username(username, conn)
        return user.user_id if user else None

    def list_recipients(self) -> list[tuple[str, str]]:
        with databricks_connection() as conn:
            users = self._user_repository.list_users(conn)
        return [(user.user_id, user.username) for user in users]

    def list_saved_filters(self, report_code: str, actor_user_id: str) -> SavedFilterList:
        with databricks_connection() as conn:
            my_filters = self._definition_repository.list_my_filters(actor_user_id, report_code, conn)
            shared = self._share_repository.list_shared_with_me(actor_user_id, report_code, conn)
            owners = {item.owner_user_id for item in shared}
            owner_map = {
                owner_user_id: self._user_repository.find_by_user_id(owner_user_id, conn)
                for owner_user_id in owners
            }
            for item in shared:
                owner = owner_map.get(item.owner_user_id)
                item.owner_username = owner.username if owner else None
        return SavedFilterList(my_filters=my_filters, shared_with_me=shared)

    def save_filter(
        self,
        report_code: str,
        actor_user_id: str,
        filter_name: str,
        raw_filter_payload: dict,
    ) -> str:
        command = SaveFilterCommand(
            report_code=report_code,
            owner_user_id=actor_user_id,
            filter_name=filter_name,
            filter_payload=raw_filter_payload,
        )
        payload = normalize_report_filter_payload(command.report_code, command.filter_payload)
        with databricks_connection() as conn:
            existing = self._definition_repository.find_by_name(
                actor_user_id,
                report_code,
                command.filter_name,
                conn,
            )
            if existing is not None:
                raise BusinessRuleError("Ten bo loc da ton tai trong man hinh bao cao nay.")
            filter_definition_id = str(uuid4())
            self._definition_repository.create(
                filter_definition_id=filter_definition_id,
                report_code=command.report_code,
                owner_user_id=command.owner_user_id,
                filter_name=command.filter_name,
                filter_payload=payload,
                actor_user_id=actor_user_id,
                conn=conn,
            )
        audit.log_action(
            "report_filter_save",
            actor_user_id=actor_user_id,
            report_code=report_code,
            filter_definition_id=filter_definition_id,
            result="success",
        )
        return filter_definition_id

    def update_filter(
        self,
        filter_definition_id: str,
        actor_user_id: str,
        filter_name: str,
        raw_filter_payload: dict,
    ) -> None:
        filter_name = str(filter_name or "").strip()
        if not filter_name:
            raise BusinessRuleError("Ten bo loc khong duoc de trong.")
        with databricks_connection() as conn:
            definition = self._definition_repository.find_by_id(filter_definition_id, conn)
            if definition is None or not definition.is_active:
                raise BusinessRuleError("Khong tim thay bo loc.")
            self._ensure_owner(definition.owner_user_id, actor_user_id)
            payload = normalize_report_filter_payload(definition.report_code, raw_filter_payload)

            duplicated = self._definition_repository.find_by_name(
                actor_user_id,
                definition.report_code,
                filter_name,
                conn,
            )
            if duplicated is not None and duplicated.filter_definition_id != definition.filter_definition_id:
                raise BusinessRuleError("Ten bo loc da ton tai trong man hinh bao cao nay.")

            self._definition_repository.update(
                filter_definition_id=filter_definition_id,
                filter_name=filter_name,
                filter_payload=payload,
                actor_user_id=actor_user_id,
                conn=conn,
            )

    def delete_filter(self, filter_definition_id: str, actor_user_id: str) -> None:
        with databricks_connection() as conn:
            definition = self._definition_repository.find_by_id(filter_definition_id, conn)
            if definition is None or not definition.is_active:
                raise BusinessRuleError("Khong tim thay bo loc.")
            self._ensure_owner(definition.owner_user_id, actor_user_id)
            self._definition_repository.deactivate(filter_definition_id, actor_user_id, conn)

    def share_filter(
        self,
        filter_definition_id: str,
        actor_user_id: str,
        recipient_user_ids: list[str],
    ) -> ShareFilterResult:
        normalized_recipients = sorted(
            {
                str(recipient_user_id).strip()
                for recipient_user_id in recipient_user_ids
                if str(recipient_user_id).strip() and str(recipient_user_id).strip() != actor_user_id
            }
        )
        if not normalized_recipients:
            raise BusinessRuleError("Vui long chon it nhat mot recipient hop le.")

        with databricks_connection() as conn:
            definition = self._definition_repository.find_by_id(filter_definition_id, conn)
            if definition is None or not definition.is_active:
                raise BusinessRuleError("Khong tim thay bo loc.")
            self._ensure_owner(definition.owner_user_id, actor_user_id)

            invalid_recipient_user_ids = {
                recipient_user_id
                for recipient_user_id in normalized_recipients
                if (
                    (user := self._user_repository.find_by_user_id(recipient_user_id, conn)) is None
                    or not user.is_active
                )
            }
            self._share_repository.share_with_recipients(
                filter_definition_id=filter_definition_id,
                recipient_user_ids=normalized_recipients,
                actor_user_id=actor_user_id,
                invalid_recipient_user_ids=invalid_recipient_user_ids,
                conn=conn,
            )

        audit.log_action(
            "report_filter_share",
            actor_user_id=actor_user_id,
            filter_definition_id=filter_definition_id,
            report_code=definition.report_code,
            recipients_count=len(normalized_recipients),
            result="success",
        )
        return ShareFilterResult(invalid_recipient_user_ids=sorted(invalid_recipient_user_ids))

    def revoke_share(
        self,
        filter_definition_id: str,
        actor_user_id: str,
        recipient_user_id: str,
    ) -> None:
        recipient_user_id = str(recipient_user_id or "").strip()
        if not recipient_user_id:
            raise BusinessRuleError("recipient_user_id khong hop le.")
        with databricks_connection() as conn:
            definition = self._definition_repository.find_by_id(filter_definition_id, conn)
            if definition is None or not definition.is_active:
                raise BusinessRuleError("Khong tim thay bo loc.")
            self._ensure_owner(definition.owner_user_id, actor_user_id)
            self._share_repository.revoke_share(
                filter_definition_id=filter_definition_id,
                recipient_user_id=recipient_user_id,
                actor_user_id=actor_user_id,
                conn=conn,
            )

        audit.log_action(
            "report_filter_revoke",
            actor_user_id=actor_user_id,
            filter_definition_id=filter_definition_id,
            recipient_user_id=recipient_user_id,
            report_code=definition.report_code,
            result="success",
        )

    def save_as_new(
        self,
        source_filter_definition_id: str,
        actor_user_id: str,
        new_filter_name: str,
    ) -> str:
        filter_name = str(new_filter_name or "").strip()
        if not filter_name:
            raise BusinessRuleError("Ten bo loc khong duoc de trong.")
        with databricks_connection() as conn:
            source = self._definition_repository.find_by_id(source_filter_definition_id, conn)
            if source is None or not source.is_active:
                raise BusinessRuleError("Khong tim thay bo loc nguon.")

            can_save = source.owner_user_id == actor_user_id or self._share_repository.has_active_share(
                source.filter_definition_id,
                actor_user_id,
                conn,
            )
            if not can_save:
                raise BusinessRuleError("Ban khong co quyen luu ban sao tu bo loc nay.")

            duplicated = self._definition_repository.find_by_name(
                owner_user_id=actor_user_id,
                report_code=source.report_code,
                filter_name=filter_name,
                conn=conn,
            )
            if duplicated is not None:
                raise BusinessRuleError("Ten bo loc da ton tai trong man hinh bao cao nay.")

            new_filter_definition_id = str(uuid4())
            self._definition_repository.create(
                filter_definition_id=new_filter_definition_id,
                report_code=source.report_code,
                owner_user_id=actor_user_id,
                filter_name=filter_name,
                filter_payload=source.filter_payload,
                actor_user_id=actor_user_id,
                conn=conn,
            )
        return new_filter_definition_id

    def get_share_recipients(self, filter_definition_id: str, actor_user_id: str):
        with databricks_connection() as conn:
            definition = self._definition_repository.find_by_id(filter_definition_id, conn)
            if definition is None or not definition.is_active:
                raise BusinessRuleError("Khong tim thay bo loc.")
            self._ensure_owner(definition.owner_user_id, actor_user_id)
            return self._share_repository.list_filter_recipients(filter_definition_id, conn)

    def get_apply_payload(
        self,
        filter_definition_id: str,
        actor_user_id: str,
        current_report_code: str,
    ) -> FilterApplyResult:
        with databricks_connection() as conn:
            definition = self._definition_repository.find_by_id(filter_definition_id, conn)
            if definition is None or not definition.is_active:
                raise BusinessRuleError("Khong tim thay bo loc.")

            if definition.report_code != current_report_code:
                raise BusinessRuleError("Khong the ap dung bo loc tu man hinh bao cao khac.")

            is_owner = definition.owner_user_id == actor_user_id
            has_share = self._share_repository.has_active_share(
                definition.filter_definition_id,
                actor_user_id,
                conn,
            )
            if not is_owner and not has_share:
                raise BusinessRuleError("Ban khong co quyen ap dung bo loc nay.")

        return parse_report_filter_payload(definition.report_code, definition.filter_payload)

    @staticmethod
    def _ensure_owner(owner_user_id: str, actor_user_id: str) -> None:
        if owner_user_id != actor_user_id:
            raise BusinessRuleError("Ban khong co quyen thao tac bo loc nay.")
