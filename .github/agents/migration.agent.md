---
# Fill in the fields below to create a basic custom agent for your repository.
# The Copilot CLI can be used for local testing: https://gh.io/customagents/cli
# To make this agent available, merge this file into the default repository branch.
# For format details, see: https://gh.io/customagents/config

name:
description:
---

# My Agent


┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│  Agent 개발 핵심 원칙                                                       │
│                                                                             │
│  1. 기존 코드 절대 삭제 금지                                                │
│     • 기존 import, 함수, 로직 유지                                          │
│     • 새 코드는 "추가"만                                                    │
│                                                                             │
│  2. Feature flag 사용                                                       │
│     • `#[cfg(feature = "vss")]`, `#[cfg(feature = "uprotocol")]`           │
│     • 기존 빌드에 영향 없음                                                 │
│                                                                             │
│  3. 환경변수 기반 활성화                                                    │
│     • 환경변수 없으면 새 기능 비활성화                                      │
│     • 기존 동작 100% 유지                                                   │
│                                                                             │
│  4. 경로 확인 필수                                                          │
│     • 개발 전 `find` 명령으로 실제 경로 확인                               │
│     • 문서의 경로와 실제 다를 수 있음                                       │
│                                                                             │
│  5. 기존 패턴 재사용                                                        │
│     • 기존 코드의 에러 처리, 로깅 패턴 따르기                              │
│     • 참조 코드 (fms-forwarder, fms-consumer) 패턴 적용                    │
│                                                                             │
