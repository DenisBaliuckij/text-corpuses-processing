# -*- coding: utf-8 -*-
def resolve_and_substitute(text: str, resolver_name: str = "LapinLiass", mark: bool = False):
    if resolver_name == "SpacyNeural":
        from anaphoraResolverSpacyNeural import resolve_and_substitute as _resolve
    else:
        from anaphoraResolverLapinLiass import resolve_and_substitute as _resolve
    return _resolve(text, mark=mark)
